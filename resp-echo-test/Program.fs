﻿open System
open System.Net
open System.Net.Sockets
open System.Collections.Concurrent
open SocAsyncEventArgFuncs


open FredisTypes
open FsCheck



let key1 = Gen.constant (Key "key1")
let key2 = Gen.constant (Key "key2")
let key3 = Gen.constant (Key "key3")
let key4 = Gen.constant (Key "key4")
let key5 = Gen.constant (Key "key5")
let key6 = Gen.constant (Key "key6")
let key7 = Gen.constant (Key "key7")
let key8 = Gen.constant (Key "key8")
let genKey = Gen.frequency[(1, key1); (1, key2); (1, key3); (1, key4); (1, key5); (1, key6); (1, key7); (1, key8) ]


let private maxByteOffset = (pown 2 29) - 1 // zero based, hence the -1
let private minByteOffset = (pown 2 29) * -1 


let genByteOffset = 
    Gen.choose(minByteOffset, maxByteOffset)
    |> Gen.map FredisTypes.ByteOffset.Create
    |> Gen.map (fun optBoffset -> optBoffset.Value)


let genAlphaByte = Gen.choose(97,122) |> Gen.map byte     
let genAlphaByteArray = Gen.arrayOfLength 8 genAlphaByte 
let genFredisCmd = Arb.generate<FredisCmd>



//let genPopulatedBulkStringContents =
//    gen{
//        let! bsl = Arb.generate<byte> |> Gen.nonEmptyListOf
//        let bs = bsl |> Array.ofList
//        return BulkStrContents.Contents bs
//    }

// containing all byte values, not overridden by genAlphaByteArray
// ensure some crlfs are embedded
let genPopulatedBulkStringContentsIncCrlf =
    let crlf = [13uy; 10uy]
    gen{
        let! bs1 = Arb.generate<byte list>
        let! bs2 = Arb.generate<byte list>
        let! bs3 = Arb.generate<byte list>
        let bsl = bs1 @ crlf @ bs2 @ crlf @ bs3
        let bs = bsl |> Array.ofList
        return BulkStrContents.Contents bs
    }

let genBulkStringContents = Gen.frequency[ (16, genPopulatedBulkStringContentsIncCrlf); (1, Gen.constant(BulkStrContents.Nil)) ]

type ArbOverrides() =
    //static member Float() =
    //    Arb.Default.Float()
    //    |> Arb.filter (fun f -> not <| System.Double.IsNaN(f) && 
    //                            not <| System.Double.IsInfinity(f) &&
    //                            (System.Math.Abs(f) < (System.Double.MaxValue / 2.0)) &&
    //                            (System.Math.Abs(f) > 0.00001 ) )
    static member Key() = Arb.fromGen genKey
    static member ByteOffsets() = Arb.fromGen genByteOffset
    static member Bytes() = Arb.fromGen genAlphaByteArray
    static member FredisCmd() = Arb.fromGen genFredisCmd 
    static member BulkStrContents() = Arb.fromGen genBulkStringContents

Arb.register<ArbOverrides>() |> ignore

let maxNumConnections = 4
let saeaBufSize = 32*1024
let saeaSharedBuffer = Array.zeroCreate<byte> (maxNumConnections * saeaBufSize)

let saeaPool = new ConcurrentStack<SocketAsyncEventArgs>()

for ctr = (maxNumConnections - 1) downto 0 do
    let saea   = new SocketAsyncEventArgs()
    let offset = ctr*saeaBufSize
    saea.SetBuffer(saeaSharedBuffer, offset, saeaBufSize)
    saea.add_Completed (fun _ b -> SocAsyncEventArgFuncs.OnClientIOCompleted b)
    saeaPool.Push(saea)





let ClientListenerLoop (client:Socket, saea:SocketAsyncEventArgs) : unit =

    let userTok:UserToken = {
        Socket = client
        ClientBuf = null
        ClientBufPos = Int32.MaxValue
        SaeaBufStart = 0
        SaeaBufEnd = 0
        SaeaBufSize = saeaBufSize
        Continuation = -1
        BufList = Collections.Generic.List<byte[]>()
        okContBytes = ignore
        okContUnit = ignore
        exnCont = ignore
        cancCont = ignore
        }

    saea.UserToken <- userTok

    // todo: consider F# anonymous classes for implenting IFredisStreamSource and IFredisStreamSink
    let saeaSrc     = SaeaStreamSource saea :> ISaeaStreamSource  
    let saeaSink    = SaeaStreamSink saea   :> ISaeaStreamSink 

    // parses bytes received to give Resp, i.e the Resp DU, then converts the Resp DU back into bytes and returns.
    let asyncProcessClientRequests = 
        async{ 
            while (client.Connected ) do
                let! bb = SocAsyncEventArgFuncs.AsyncReadByte saea
                let  respTypeInt = System.Convert.ToInt32 bb
                let! resp = SaeaAsyncRespMsgParser.LoadRESPMsg respTypeInt saeaSrc
                SocAsyncEventArgFuncs.Reset(saea)
                do! SaeaAsyncRespStreamFuncs.AsyncSendResp saeaSink resp
                do! saeaSink.AsyncFlush ()
                SocAsyncEventArgFuncs.Reset saea
            }

    Async.StartWithContinuations(
            asyncProcessClientRequests,
            (fun () ->  saeaPool.Push saea ),
            (fun ex ->  saeaPool.Push saea ),
            (fun _  ->  saeaPool.Push saea )
        ) // end Async




let rec ProcessAccept (saeaAccept:SocketAsyncEventArgs) = 
    let listenSocket = saeaAccept.UserToken :?> Socket
    match saeaPool.TryPop() with
    | true, saea    ->  let clientSoc = saeaAccept.AcceptSocket // use clientSocket causes immediate disposal when the variable goes out of scope
                        ClientListenerLoop(clientSoc, saea)
    | false, _      ->  use clientSoc = saeaAccept.AcceptSocket
                        clientSoc.Send ErrorMsgs.maxNumClientsReached |> ignore
                        clientSoc.Disconnect false
    StartAccept listenSocket saeaAccept
and StartAccept (listenSocket:Socket) (acceptEventArg:SocketAsyncEventArgs) =
    acceptEventArg.AcceptSocket <- null
    let ioPending = listenSocket.AcceptAsync acceptEventArg
    if not ioPending then
        ProcessAccept acceptEventArg


//let private sendReceive (tcpClient:TcpClient) (msg:Resp) =
//    let strm = tcpClient.GetStream()
//    RespStreamFuncs.AsyncSendResp strm msg |> Async.RunSynchronously
//    let respTypeInt = strm.ReadByte()
//    RespMsgParser.LoadRESPMsg tcpClient.ReceiveBufferSize respTypeInt strm

let host = "127.0.0.1"
let port = 6379
let ipAddr        = IPAddress.Parse(host)
let localEndPoint = IPEndPoint (ipAddr, port)

let listenSocket = new Socket(localEndPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp)
listenSocket.Bind(localEndPoint)
listenSocket.Listen 16
let acceptEventArg = new SocketAsyncEventArgs()
acceptEventArg.UserToken <- listenSocket
acceptEventArg.add_Completed (fun _ saea -> ProcessAccept saea)
StartAccept listenSocket acceptEventArg



let makeResp (resp:FredisTypes.Resp) =
    printfn "%O" resp
    true


let propRespSentToEchoServerReturnsSame (cmd:FredisTypes.FredisCmd) =
    //printfn "%A" cmd
    let respIn = cmd |> (FredisCmdToResp.FredisCmdToRESP >> FredisTypes.Resp.Array)
    use tcpClient = new TcpClient(host, port)
    let strm = tcpClient.GetStream()
    AsyncRespStreamFuncs.AsyncSendResp strm respIn |> Async.RunSynchronously
    let respTypeInt = strm.ReadByte()
    let respOut = RespMsgParser.LoadRESPMsg tcpClient.ReceiveBufferSize respTypeInt strm
    respIn = respOut


//let config =  {FsCheck.Config.Verbose with EndSize = 999}
//Check.One (config, makeResp)

Check.Verbose makeResp




printfn "tests complete"
System.Console.ReadKey() |> ignore