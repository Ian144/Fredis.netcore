[<RequireQualifiedAccess>]
module RespMsgPipelineReader

open System.IO
open FredisTypes
open System.Buffers

//open Utils


// In pattern matching expressions, identifiers that begin with lowercase characters are always treated as 
// variables to be bound, rather than as literals, so you should generally use initial capitals when you define literals
// https://msdn.microsoft.com/en-us/library/dd233193.aspx

[<Literal>]
let SimpleStringL = 43  // +

[<Literal>]
let ErrorL = 45         // -

[<Literal>]
let IntegerL = 58       // :

[<Literal>]
let BulkStringL = 36    // $

[<Literal>]
let ArrayL = 42         // *

[<Literal>]
let CR = 13

[<Literal>]
let LF = 10


//// TODO reading a single byte at at time is probably inefficient, consider a more efficient version of this function
//let rec ReadUntilCRLF (strm:SequenceReader<byte>) : int list = 
//    match strm.ReadByte() with    // annoyingly ReadByte returns an int32
//    | -1    ->  failwith "ReadUntilCRLF EOS before CRLF"
//    | CR    ->  SequenceReader<byte>Funcs.Eat1 strm         // assuming the next char is LF, and eating it
//               []
//    | b     ->  b :: (ReadUntilCRLF strm ) 


//let ReadDelimitedResp (makeRESPMsg:Bytes -> Resp) (strm:SequenceReader<byte>) : Resp = 
//    let bs = ReadUntilCRLF strm
//    bs |> List.map byte |> Array.ofList |> makeRESPMsg


//// an imperative int64 reader, adapted from sider code
//let inline ReadInt64 (strm:SequenceReader<byte>) = 
//    let mutable num =0L
//    let mutable b = strm.ReadByte()
//    if b = 45 then // if first byte is a minus sign
//        b <- strm.ReadByte()
//        while b <> CR do
//            num <- num * 10L + (int64 b) - 48L
//            b <- strm.ReadByte()
//        strm.ReadByte() |> ignore // throw away the CRLF
//        num * -1L
//    else
//        while b <> CR do
//            num <- num * 10L + (int64 b) - 48L
//            b <- strm.ReadByte()
//        strm.ReadByte() |> ignore // throw away the CRLF
//        num


//// an imperative int64 reader
//// adapted from sider code
//let inline ReadInt32 (strm:SequenceReader<byte>) = 
//    let mutable num =0
//    let mutable b = strm.ReadByte()
//    if b = 45 then // if first byte is a '-' minus sign
//        b <- strm.ReadByte()
//        while b <> CR do
//            num <- num * 10 + b - 48
//            b <- strm.ReadByte()
//        strm.ReadByte() |> ignore // throw away the CRLF
//        num * -1
//    else
//        while b <> CR do
//            num <- num * 10 + b - 48
//            b <- strm.ReadByte()
//        strm.ReadByte() |> ignore // throw away the CRLF
//        num 


//let ReadBulkString(rcvBufSz:int) (strm:SequenceReader<byte>) = 
//    let rec readInner (strm:PipeRader) (totalBytesToRead:int) (byteArray:byte array) =
//        let mutable maNumBytesToRead = if totalBytesToRead > rcvBufSz then rcvBufSz else totalBytesToRead
//        let mutable totalSoFar = 0
//        while totalSoFar < totalBytesToRead do
//            let numBytesRead = strm.Read (byteArray, totalSoFar, maxNumBytesToRead)            
//            totalSoFar <- totalSoFar + numBytesRead
//            let numBytesRemaining = totalBytesToRead - totalSoFar
//            maxNumBytesToRead <- if numBytesRemaining > rcvBufSz then rcvBufSz else numBytesRemaining
//    let lenToRead = ReadInt32 strm
//    match lenToRead with
//    | -1    ->  Resp.BulkString BulkStrContents.Nil
//    | len   ->  let byteArr = Array.zeroCreate<byte> len
//                do readInner strm  len byteArr
//                strm.ReadByte() |> ignore   // eat CR
//                strm.ReadByte() |> ignore   // eat LF
//                byteArr |> RespUtils.MakeBulkStr


//let ReadRESPInteger = ReadInt64 >> Resp.Integer 
    

//let rec LoadRESPMsgArray (rcvBuffSz:int) (buffer:SequenceReader<byte>) = 
//    let arrSz = ReadInt32 buffer
//    let msgs = Array.zeroCreate<Resp> arrSz
//    let maxIdx = arrSz - 1
//    for idx = 0 to maxIdx do
//        let resp = LoadRESPMsgInner rcvBuffSz buffer
//        msgs.[idx] <- resp
//    Resp.Array msgs

//and LoadRESPMsgInner (rcvBuffSz:int) (sr:SequenceReader<byte>) = 
//    let mutable bb = 0uy
//    let respTypeByte = sr.TryPeek( &bb )
//    LoadRESPMsg rcvBuffSz respTypeByte sr 

//and LoadRESPMsg (rcvBufSz:int) (respType:int) (sr:SequenceReader<byte>) = 
//    match respType wit
//    | SimpleStringL ->  ReadDelimitedResp Resp.SimpleString sr
//    | ErrorL        ->  ReadDelimitedResp Resp.Error sr
//    | IntegerL      ->  ReadRESPInteger sr
//    | BulkStringL   ->  ReadBulkString rcvBufSz sr
//    | ArrayL        ->  LoadRESPMsgArray rcvBufSz sr
//    | _             ->  let bs = Array.zeroCreate<byte> 16
//                        sr.Read(bs, 0, 16) |> ignore
//                        let str = bs |> Utils.BytesToStr  
//                        let msg = sprintf "invalid RESP: %d - %s" respType str
//                        failwith msg

