package dev.claviron.orbit;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/** Application-owned service Link with its own single reader and invoke bounds. */
public final class OrbitInvocations implements AutoCloseable {
    @FunctionalInterface public interface Handler { CompletionStage<InvokeReply> handle(OrbitInvocation request) throws Exception; }
    final OrbitApplication application;
    final InvokeFfm api;
    private final long handle;
    private final Object calls=new Object(), pending=new Object(), closeLock=new Object();
    private final CompletableFuture<Void> serving=new CompletableFuture<>();
    private Thread intake;
    private boolean withdrawn, released;
    private int active;
    OrbitInvocations(OrbitApplication application,long applicationHandle,InvokeOptions options) {
        this.application=application; api=new InvokeFfm(application.runtime.library.ffi); handle=api.open(applicationHandle,options);
    }
    public InvokeCall call(String target,String operation,byte[] payload) {
        synchronized(calls) {
            if(withdrawn) throw new NativeException(6);
            NativeOperation nativeCall=application.runtime.submit(ignored->api.call(handle,target,operation,payload));
            return new InvokeCall(nativeCall,api);
        }
    }
    public CompletionStage<OrbitInvocation> next() {
        synchronized(calls) {
            if(withdrawn) return CompletableFuture.completedStage(null);
            NativeOperation operation=application.runtime.submit(ignored->api.operation(api.next,handle));
            return operation.completion().thenApply(ignored->{
                if(operation.resource==0) return null;
                try {return new OrbitInvocation(this,operation.resource);}
                catch(Throwable failure) {Ffm.check(application.runtime.library.ffi.one(api.requestRelease,operation.resource));throw failure;}
            });
        }
    }
    public CompletionStage<Void> serve(Handler handler,Executor executor) {
        synchronized(calls) {
            if(intake!=null || withdrawn) throw new IllegalStateException("Invoke intake already started or closed");
            intake=Thread.ofPlatform().name("orbit-java-invokes").start(()->{
                Throwable failure=null;
                try {
                    for(;;) {
                        OrbitInvocation request=next().toCompletableFuture().join(); if(request==null) break;
                        synchronized(pending) {active++;}
                        try {executor.execute(()->handle(request,handler));} catch(Throwable error) {finish(request);}
                    }
                } catch(Throwable error) {failure=OrbitRequest.unwrap(error);withdraw();}
                finally {
                    boolean interrupted=false;
                    synchronized(pending) {while(active!=0) {try {pending.wait();}catch(InterruptedException signal){interrupted=true;}}}
                    if(interrupted) Thread.currentThread().interrupt();
                    if(failure==null) serving.complete(null);else serving.completeExceptionally(failure);
                }
            });
        }
        return serving.minimalCompletionStage();
    }
    private void handle(OrbitInvocation request,Handler handler) {
        AtomicBoolean retired=new AtomicBoolean();
        Runnable retire=()->{if(retired.compareAndSet(false,true))finish(request);};
        request.cancelled().whenComplete((ignored,failure)->retire.run());
        if(retired.get() || !request.current()){retire.run();return;}
        CompletionStage<InvokeReply> reply;
        try {reply=handler.handle(request);if(reply==null)throw new IllegalStateException("Handler returned no invoke stage");}
        catch(Throwable failure){reply=CompletableFuture.failedStage(failure);}
        reply.whenComplete((value,failure)->{
            if(retired.get())return;
            try {request.reply(failure==null && value!=null?value:InvokeReply.rejected(new byte[0]));}
            finally {retire.run();}
        });
    }
    private void finish(OrbitInvocation request) {
        try {request.close();}
        finally {request.terminal().whenComplete((ignored,failure)->{synchronized(pending){active--;pending.notifyAll();}});}
    }
    public void withdraw() {
        synchronized(calls) {if(!withdrawn){Ffm.check(application.runtime.library.ffi.one(api.withdraw,handle));withdrawn=true;}}
    }
    @Override public void close() {
        application.runtime.requireExternalCloser();
        if(Thread.currentThread()==intake)throw new IllegalStateException("Cannot join own invoke intake");
        synchronized(closeLock) {
            if(released)return; withdraw(); join(intake);
            NativeOperation closed=application.runtime.submit(ignored->api.operation(api.closed,handle));
            Throwable failure=null;
            try {closed.completion().toCompletableFuture().join();}catch(Throwable error){failure=OrbitRequest.unwrap(error);}
            synchronized(calls){Ffm.check(application.runtime.library.ffi.one(api.release,handle));released=true;}
            application.detachInvocations(this);
            if(failure!=null)throw Ffm.unexpected(failure);
            if(intake!=null)serving.join();
        }
    }
    private static void join(Thread thread) {
        if(thread==null)return;boolean interrupted=false;
        for(;;){try{thread.join();break;}catch(InterruptedException signal){interrupted=true;}}
        if(interrupted)Thread.currentThread().interrupt();
    }
}
