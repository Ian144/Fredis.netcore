open System
open System.Net
open System.Net.Sockets
open System.Threading
open System.IO.Pipelines;
open System.Buffers

let host = """0.0.0.0"""
let port = 6379

let minBufSize = 8 * 1024

let mutable clientCount:int = 0



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

let LoopReadPipe (client:Socket) (pipeReader:PipeReader) (ct:CancellationToken) =
    let mutable loopAgain = true
    async{
        while loopAgain do
            let readAsyncTask = pipeReader.ReadAsync(ct).AsTask()
            let! result = Async.AwaitTask readAsyncTask
            if result.IsCompleted then
                loopAgain <- false
            else
                let buffer = result.Buffer

                // see https://blog.marcgravell.com/2018/07/pipe-dreams-part-1.html
                //buffer.IsSingleSegment
                //buffer.First

                let eol:byte = '\n'B
                let xx = "\r\n"B
                let nullableEOLPosition = buffer.PositionOf( eol )
                ()
            ()
    }






let SetupSingleClientHandler (client:Socket) (ct:CancellationToken) =
    //clientCount <- clientCount + 1 
    printf "new client, num: %d" clientCount
    //let options = new PipeOptions(  readerScheduler: PipeScheduler.Inline, writerScheduler: PipeScheduler.Inline, useSynchronizationContext: false)

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
