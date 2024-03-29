﻿
module SaeaAsyncRespStreamFuncs

open System
open System.IO
open FredisTypes

open SocAsyncEventArgFuncs






let private crlf        = "\r\n"B
let private simpStrType = "+"B
let private errStrType  = "-"B
let nilBulkStrBytes     = "$-1\r\n"B



let private AsyncSendBulkString (strm:ISaeaStreamSink) (contents:BulkStrContents) =

    match contents with
    | BulkStrContents.Contents bs   ->  let prefix = (sprintf "$%d\r\n" bs.Length) |> Utils.StrToBytes
                                        async{
                                            do! strm.AsyncWrite prefix
                                            do! strm.AsyncWrite bs
                                            do! strm.AsyncWrite crlf
                                        }
    | BulkStrContents.Nil         ->    async{ do! strm.AsyncWrite nilBulkStrBytes }





let private AsyncSendSimpleString (strm:ISaeaStreamSink) (contents:byte array) =
    async{
        do! strm.AsyncWrite simpStrType
        do! strm.AsyncWrite contents
        do! strm.AsyncWrite crlf 
    }


let AsyncSendError (strm:ISaeaStreamSink) (contents:byte array) =
    async{
        do! strm.AsyncWrite errStrType
        do! strm.AsyncWrite contents
        do! strm.AsyncWrite crlf 
    }


let private AsyncSendInteger (strm:ISaeaStreamSink) (ii:int64) =
    let bs = sprintf ":%d\r\n" ii |> Utils.StrToBytes
    strm.AsyncWrite bs
    

let rec AsyncSendResp (strm:ISaeaStreamSink) (msg:Resp) =
    match msg with
    | Resp.Array arr            -> AsyncSendArray strm arr
    | Resp.BulkString contents  -> AsyncSendBulkString strm contents
    | Resp.SimpleString bs      -> AsyncSendSimpleString strm bs
    | Resp.Error err            -> AsyncSendError strm err
    | Resp.Integer ii           -> AsyncSendInteger strm ii

and private AsyncSendArray (strm:ISaeaStreamSink) (arr:Resp []) =
    let lenBytes = sprintf "*%d\r\n" arr.Length |> Utils.StrToBytes
    let ctr = ref 0
    async{
        do! strm.AsyncWrite( lenBytes)
        while !ctr < arr.Length do
            do! AsyncSendResp strm arr.[!ctr]
            ctr := !ctr + 1
    }
