module TestRedisVsFredis

open Xunit
open Swensen.Unquote

open CmdCommon
open FredisTypes


// FIX DRY violation
let private readRESPInteger (msg:Resp) = 
    match msg with
    | Resp.Integer ii   ->  ii
    | _                 ->  failwith "non integer RESP passed to readRESPInteger"


//  redis               fredis
//  Some (Integer 0L), Some (Integer 0L))
//  Some (Integer 1L), Some (Integer 1L))
//  Some (Integer -1L), Some (Integer 8L))
// 
// SetBit key1 0 0
// BitOp NOT key2 key1
// Bitpos key2 0 0 0

[<Fact>]
let ``setbit bitop bitpos 1`` () =
    let key1 = Key "key1"
    let key2 = Key "key2"
    let hashMap = HashMap()


    let setbitCmd   = FredisCmd.SetBit (key1, 0ul, false)

    let bitopNot    = BitOpInner.NOT (key2, key1)
    let bitOpCmd    = FredisCmd.BitOp bitopNot

    let byteoffsetZero  = (ByteOffset.Create 0).Value
    let range           = ArrayRange.LowerUpper (byteoffsetZero, byteoffsetZero)
    let bitposCmd       = FredisCmd.Bitpos (key2, false, range) 



    FredisCmdProcessor.Execute hashMap setbitCmd |> ignore
    FredisCmdProcessor.Execute hashMap bitOpCmd |> ignore
    let respResult = FredisCmdProcessor.Execute hashMap bitposCmd

    test <@ -1L = (readRESPInteger respResult)  @>

[<Fact>]
let ``setbit bitop bitpos 2`` () =
    let key1 = Key "key1"
    let key2 = Key "key2"

    let hashMap = HashMap()

    let setbitCmd   = FredisCmd.SetBit (key1, 0ul, false)
    
    let bitopNot    = BitOpInner.NOT (key2, key1)
    let bitOpCmd    = FredisCmd.BitOp bitopNot

    let bitposCmd   = FredisCmd.Bitpos (key2, false, ArrayRange.All) 

    FredisCmdProcessor.Execute hashMap setbitCmd |> ignore
    FredisCmdProcessor.Execute hashMap bitOpCmd |> ignore
    let respResult = FredisCmdProcessor.Execute hashMap bitposCmd

    test <@ 8L = (readRESPInteger respResult)  @>