package example;

import dev.claviron.orbit.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

/** Plain JDK invoke correctness fixtures; never packaged in the production JAR. */
public final class InvokeApplication {
    private InvokeApplication() {}
    private static void check(boolean value,String reason){if(!value)throw new AssertionError(reason);}
    private static synchronized void signal(String value){System.out.println(value);System.out.flush();}
    private static byte[] bytes(ByteBuffer value){byte[] bytes=new byte[value.remaining()];value.get(bytes);return bytes;}
    private static void code(InvokeResult result,InvokeResult.Code expected){check(result.code()==expected,"Expected "+expected+", got "+result.code());}
    private static void cancelled(InvokeCall call){
        try {call.completion().toCompletableFuture().join();throw new AssertionError("Call was not cancelled");}
        catch(CompletionException error){check(error.getCause() instanceof CancellationException,"Wrong cancellation: "+error);}
        catch(CancellationException expected){/* CompletableFuture may expose cancellation directly. */}
    }
    @SuppressWarnings("try") // Early application close tests owned invoke drain; resource scope remains failure cleanup.
    public static void main(String[] args)throws Exception{
        if(args.length!=4)throw new IllegalArgumentException("usage: InvokeApplication <library> <application-fleet> <invoke-fleet> <response-bound>");
        int bound=Integer.parseInt(args[3]);
        AtomicReference<CompletableFuture<InvokeReply>> held=new AtomicReference<>();
        AtomicReference<OrbitInvocation> heldRequest=new AtomicReference<>();
        AtomicReference<InvokeCall> outgoing=new AtomicReference<>();
        try(NativeLibrary library=NativeLibrary.load(Path.of(args[0]));
            NativeRuntime runtime=library.openRuntime(96,bound*16L,2);
            var executor=Executors.newVirtualThreadPerTaskExecutor();
            OrbitApplication application=runtime.join(new ApplicationOptions(args[1],"demo",17,8,4,8))){
            OrbitInvocations invokes=application.invocations(new InvokeOptions(args[2],"java",4,1,32,1024,bound,
                    Map.of("native","rust","missing","absent","ambiguous","dupe")));
            invokes.serve(request->{
                switch(request.operation()){
                    case "echo", "echo-λ" -> {return CompletableFuture.completedStage(InvokeReply.success(bytes(request.payload())));}
                    case "generation" -> {return CompletableFuture.completedStage(InvokeReply.success(Long.toString(request.generation()).getBytes(StandardCharsets.UTF_8)));}
                    case "reject" -> {return CompletableFuture.completedStage(InvokeReply.rejected(new byte[]{0,(byte)255,7}));}
                    case "fail" -> throw new IllegalStateException("Fixture handler failure");
                    case "too-big" -> {return CompletableFuture.completedStage(InvokeReply.success(new byte[bound+1]));}
                    case "hold" -> {
                        var future=new CompletableFuture<InvokeReply>();held.set(future);heldRequest.set(request);
                        signal("INBOUND_HELD "+request.generation());return future;
                    }
                    case "drain" -> {
                        return invokes.call("native","gate",bytes(request.payload())).completion().thenApply(result->{
                            code(result,InvokeResult.Code.SUCCESS);return InvokeReply.success(bytes(result.payload()));
                        });
                    }
                    default -> {return CompletableFuture.completedStage(InvokeReply.unsupported());}
                }
            },executor);
            application.serve(request->CompletableFuture.completedStage(OrbitResponse.text("http-ready")),executor);
            application.controls(control->{
                if(control.kind()==OrbitControl.Kind.LIFECYCLE)signal("CONTROL "+control.action()+" "+control.generation());
            },executor);
            signal("INVOKE_READY "+ProcessHandle.current().pid());
            var input=new java.io.BufferedReader(new java.io.InputStreamReader(System.in,StandardCharsets.UTF_8));
            for(String line;(line=input.readLine())!=null && !line.equals("stop");){
                switch(line){
                    case "outgoing-basic" -> {
                        byte[] payload={0,(byte)255,9};
                        InvokeCall call=invokes.call("native","echo",payload);Arrays.fill(payload,(byte)99);
                        InvokeResult result=call.completion().toCompletableFuture().join();code(result,InvokeResult.Code.SUCCESS);
                        check(Arrays.equals(bytes(result.payload()),new byte[]{0,(byte)255,9}),"Submitted bytes changed");
                        code(invokes.call("unknown","echo",new byte[0]).completion().toCompletableFuture().join(),InvokeResult.Code.UNKNOWN_TARGET);
                        code(invokes.call("missing","echo",new byte[0]).completion().toCompletableFuture().join(),InvokeResult.Code.UNAVAILABLE);
                        code(invokes.call("ambiguous","echo",new byte[0]).completion().toCompletableFuture().join(),InvokeResult.Code.AMBIGUOUS_TARGET);
                        code(invokes.call("native","echo",new byte[1025]).completion().toCompletableFuture().join(),InvokeResult.Code.PAYLOAD_TOO_LARGE);
                        InvokeResult rejected=invokes.call("native","reject",new byte[0]).completion().toCompletableFuture().join();
                        code(rejected,InvokeResult.Code.REJECTED);check(Arrays.equals(bytes(rejected.payload()),new byte[]{0,(byte)255}),"Rejected bytes changed");
                        signal("OUTGOING_BASIC_PASSED");
                    }
                    case "finish-hold" -> {
                        check(!heldRequest.get().cancelled().toCompletableFuture().isDone(),"Caller cancellation was treated as remote rollback");
                        held.get().complete(InvokeReply.success(new byte[]{7}));signal("HOLD_FINISHED");
                    }
                    case "finish-late" -> {
                        held.get().complete(InvokeReply.success(new byte[]{8}));
                        try{heldRequest.get().reply(InvokeReply.success(new byte[]{9}));throw new AssertionError("Old invocation remained writable");}
                        catch(CancellationException expected){/* Retired Java scope. */}
                        signal("LATE_DROPPED");
                    }
                    case "call-gate" -> {outgoing.set(invokes.call("native","gate",new byte[]{0,(byte)255}));signal("OUTGOING_STARTED");}
                    case "cancel-outgoing" -> {check(outgoing.get().cancel(),"Native call was already terminal");cancelled(outgoing.get());signal("OUTGOING_CANCELLED");}
                    case "assert-cancelled" -> {cancelled(outgoing.get());signal("GENERATION_CANCELLED");}
                    case "restart" -> application.restart();
                    case "close-application" -> {application.withdraw();signal("APP_WITHDRAWN");application.close();signal("APP_CLOSED");}
                    default -> throw new IllegalArgumentException("Unknown invoke fixture control");
                }
            }
        }
        signal("INVOKE_DRAINED");
    }
}
