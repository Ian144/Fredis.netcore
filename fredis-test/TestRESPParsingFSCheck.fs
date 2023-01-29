module TestRESPParsingFSCheck

#nowarn "21"
#nowarn "40"


open System.IO
open Xunit
open FsCheck
open FsCheck.Xunit
open System.IO.Pipelines
open System.Buffers

open Utils
open CmdCommon
open FredisTypes

[<Literal>]
let BulkStringL = 36    // $


[<Literal>]
let CR = 13

[<Literal>]
let LF = 10


// 65, and 122 are the lowest and highest alpha characters
// genAlphaByteArray is used to create the contents of Resp SimpleString's and Errors, which cannot contain CRLF
let genAlphaByte = Gen.choose(65,122) |> Gen.map byte 

let genAlphaByteArray = Gen.arrayOf genAlphaByte 

let genPopulatedRespBulkString = 
    let crlf = [13uy; 10uy]
    gen{
        let! bs1 = Arb.generate<byte list>
        let! bs2 = Arb.generate<byte list>
        let! bs3 = Arb.generate<byte list>
        let bsl = bs1 @ crlf @ bs2 @ crlf @ bs3
        let bs = bsl |> Array.ofList
        return RespUtils.MakeBulkStr bs
    }

let genNilRespBulkStr = gen{ return Resp.BulkString BulkStrContents.Nil }

let genRespBulkString = Gen.frequency [10, genPopulatedRespBulkString;  1,  genNilRespBulkStr]

let genRespSimpleString = 
    gen{
        let! bytes = genAlphaByteArray
        return Resp.SimpleString bytes
    }

let genRespError = 
    gen{
        let! bytes = genAlphaByteArray
        return Resp.Error bytes
    }


let genRespInteger = 
    gen{
        let! ii = Arb.Default.Int64().Generator
        return Resp.Integer ii
    }


let rec genRespArray = 
    gen{
        let! elements = Gen.arrayOfLength 4 genResp
        return Resp.Array elements
    }
and genResp = Gen.frequency [ 1, genRespSimpleString; 1, genRespError; 2, genRespInteger; 2, genRespBulkString; 1, genRespArray]


//ArbResp makes valid RESP only, hence does not test response to invalid resp
type ArbOverrides = 
    static member Resp() = Arb.fromGen (genResp )
    static member Ints() =
        Arb.fromGen (Gen.choose(1, 8096*8096))


type RoundTripPropertyAttribute() = 
    inherit PropertyAttribute(
        Arbitrary = [| typeof<ArbOverrides> |],
        MaxTest = 999,
        EndSize = 999
    )


[<RoundTripProperty>]
let ``Async Write-Read Resp stream roundtrip`` (respIn:FredisTypes.Resp) =
    use strm = new System.IO.MemoryStream()
    AsyncRespStreamFuncs.AsyncSendResp strm respIn |> Async.RunSynchronously
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    let respOut = AsyncRespMsgParser.LoadRESPMsgInner (8*1024) strm |> Async.RunSynchronously
    let isEof = strm.Position = strm.Length
    respIn = respOut && isEof


[<RoundTripProperty>]
let ``Write-Read Resp stream roundtrip`` (bufSize:int) (respIn:Resp) =
    use strm = new MemoryStream()
    RespStreamFuncs.SendResp strm respIn
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    let respTypeByte = strm.ReadByte() 
    let respOut = RespMsgParser.LoadRESPMsg bufSize respTypeByte strm
    let isEof = strm.Position = strm.Length
    respIn = respOut && isEof


[<RoundTripProperty>]
let ``Write-Read Resp PipeLine roundtrip using streams`` (bufSize:int) (respIn:Resp) =
    let options = PipeOptions( null, PipeScheduler.Inline, PipeScheduler.Inline, -1L, -1L, -1, false )
    let pipe = Pipe(options);
    let writer = pipe.Writer
    use writeStrm = writer.AsStream(true)
    RespStreamFuncs.SendResp writeStrm respIn
    let reader = pipe.Reader
    use readStrm = reader.AsStream(true)
    let respTypeByte = readStrm.ReadByte() 
    let respOut = RespMsgParser.LoadRESPMsg bufSize respTypeByte readStrm
    respIn = respOut


[<RoundTripProperty>]
let ``ReadInt64 Write-Read roundtrip`` (ii:int64) =
    use strm = new MemoryStream()
    let bytes = (sprintf "%d\r\n" ii) |> Utils.StrToBytes
    strm.Write (bytes, 0, bytes.Length) |> ignore
    strm.Seek(0L, System.IO.SeekOrigin.Begin) |> ignore
    let iiOut = RespMsgParser.ReadInt64(strm)
    let isEof = strm.Position = strm.Length
    ii = iiOut && isEof




