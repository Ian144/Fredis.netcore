open System
open System.Net
open System.Net.Sockets
open System.Threading
open System.IO.Pipelines;

let host = """0.0.0.0"""
let port = 6379

let minBufSize = 8 * 1024

let mutable clientCount:int = 0

// todo: remove this
[<Literal>]
let PingL = 80  // P - redis-benchmark PING_INLINE just sends PING\r\n, not encoded as RESP
let pongBytes  = "+PONG\r\n"B

//#nowarn "52"
let WaitForExitCmd () = 
    while stdin.Read() <> 88 do // 88 is 'X'
        ()


let LoopReadSocketWriteIntoPipeAsync (client:Socket) (pipeWriter:PipeWriter) (ct:CancellationToken) =
    let mutable loopAgain = true
    async{
        // todo: is there a more functional way to avoid 'while loopAgain do'?
        while loopAgain do
            try

                let pipeMemory = pipeWriter.GetMemory(minBufSize);
            
                // todo, is there a better way to wait on ValueTask
                let bytesReadTask = client.ReceiveAsync(pipeMemory, SocketFlags.None, ct).AsTask()
                let! bytesRead = Async.AwaitTask bytesReadTask

                if bytesRead = 0 then
                    loopAgain <- false
                else
                    pipeWriter.Advance(bytesRead)
                    let flushResultTask = pipeWriter.FlushAsync(ct).AsTask()
                    let! flushResult = Async.AwaitTask flushResultTask
                    if flushResult.IsCompleted then
                        loopAgain <- false
                        //todo< should pipeWriter.Completed(Async) be called here
            with 
                | :? OperationCanceledException ->  loopAgain <- false
    }

let LoopReadPipe (client:Socket) (pipe:Pipe) (pipeReader:PipeReader) (ct:CancellationToken) =
    let mutable loopAgain = true

    let pipeReader2 = pipe.Reader;

    async{
        while loopAgain do
            let readAsyncTask = pipeReader.ReadAsync(ct).AsTask()
            let! result = Async.AwaitTask readAsyncTask
            if result.IsCompleted then
                loopAgain <- false
            else
                // false: don't complete the pipeline
                use readStream = pipeReader.AsStream(false);
                let respTypeInt = readStream.ReadByte()
                if respTypeInt = PingL then // PING_INLINE cmds are sent as PING\r\n - i.e. a raw string not RESP (PING_BULK is RESP)
                    RespStreamFuncs.Eat5NoAlloc readStream  
                    readStream.Write (pongBytes, 0, pongBytes.Length)
                    do! readStream.FlushAsync() |> Async.AwaitTask // todo: does the pipe based stream need to be flushed
                else
                    let respMsg = RespMsgParser.LoadRESPMsg client.ReceiveBufferSize respTypeInt readStream
                    let choiceFredisCmd = FredisCmdParser.RespMsgToRedisCmds respMsg
                    match choiceFredisCmd with 
                    | Choice1Of2 cmd    ->  let! reply = CmdProcChannel.MailBoxChannel cmd // to process the cmd on a single thread
                                            RespStreamFuncs.SendResp readStream reply
                                            do! readStream.FlushAsync() |> Async.AwaitTask
                    | Choice2Of2 err    ->  do! AsyncRespStreamFuncs.AsyncSendError readStream err
                                            do! readStream.FlushAsync() |> Async.AwaitTask

                ()
            ()
    }


let SetupSingleClientHandler (client:Socket) (ct:CancellationToken) =
    clientCount <- clientCount + 1 
    printf "new client, num: %d" clientCount
    let options = PipeOptions( null, PipeScheduler.Inline, PipeScheduler.Inline, -1L, -1L, -1, false )
    let pipe = Pipe(options);

    ()


// todo: this is all very imperative, fix
let Run (listener:Socket) cancTok =
    printf "listening on %O" listener.LocalEndPoint
    let mutable loopAgain = true
    while loopAgain do
        let clientTask = listener.AcceptAsync()
        let cancelledTask = Tasks.Task.Delay(Timeout.InfiniteTimeSpan, cancTok);
        Tasks.Task.WaitAny(clientTask, cancelledTask) |> ignore;

        if cancTok.IsCancellationRequested then
            loopAgain <- false
        elif clientTask.IsCompletedSuccessfully then
            let client = clientTask.GetAwaiter().GetResult()
            SetupSingleClientHandler client cancTok;
        else
            ()
    ()


let main2 (argv:string[]) =

    let cBufSize =
        if argv.Length = 1 then
            Utils.ChoiceParseInt (sprintf "invalid integer %s" argv.[0]) argv.[0]
        else
            Choice1Of2 (8 * 1024)

    match cBufSize with
    | Choice1Of2 bufSize -> 
        printfn "buffer size: %d"  bufSize

        let ipHostInfo = Dns.GetHostEntry(host);
        let ipAddress = ipHostInfo.AddressList.[1];
        let localEndPoint = IPEndPoint(ipAddress, port);

        use listener = new Socket( localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp )
        listener.Bind(localEndPoint);
        listener.Listen(64);

        // todo: how does this interact with the global async token source?
        //       presumably the "global async token source" is for asyncs, i'm using it for a thread
        // todo: do i need to use async here, would/could async be better??
        use cts = new CancellationTokenSource();
        let ct = cts.Token;

        let thrd = new Thread( fun () -> Run listener ct );
        thrd.Start();

        printfn "fredis.net startup complete\nawaiting incoming connection requests\npress 'X' to exit"
        WaitForExitCmd ()
        printfn "cancelling asyncs"
        do Async.CancelDefaultToken()
        
        // wait until all clients have disconnected
        thrd.Join(1000) |> ignore
        printfn "stopped"
        0 // return an success code
    | Choice2Of2 msg -> printf "%s" msg
                        1 // return a failure exit code





[<EntryPoint>]
let main argv =

    let cBufSize =
        if argv.Length = 1 then
            Utils.ChoiceParseInt (sprintf "invalid integer %s" argv.[0]) argv.[0]
        else
            Choice1Of2 (8 * 1024)

    match cBufSize with
    | Choice1Of2 bufSize -> 
        printfn "buffer size: %d"  bufSize
        let ipAddr = IPAddress.Parse(host)
        let listener = TcpListener(ipAddr, port) 
        listener.Start ()
        MsgLoops.ConnectionListenerLoop bufSize listener
        printfn "fredis.net startup complete\nawaiting incoming connection requests\npress 'X' to exit"
        WaitForExitCmd ()
        do Async.CancelDefaultToken()
        printfn "cancelling asyncs"
        listener.Stop()
        printfn "stopped"
        0 // return an integer exit code
    | Choice2Of2 msg -> printf "%s" msg
                        1 // non-zero exit code
