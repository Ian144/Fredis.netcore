

# ideas 

round trip property test, 
- generate resp, 
- store resp in 
- translate to commands
- translate back to resp
- IS out == in??

what is the delimeter of an entire resp messager
raw resp bytes out in ReadonlySequence<byte>? or pipes? 
- pipes would exercise more of the code


how can i know when the entire message block has been recieved, without parsing the entire resp message

if the message is not an array of message then the msg ends with CRLF


resp array contain embedded CRLFs between each element
BulkStrings can contain embedded CRLFs



