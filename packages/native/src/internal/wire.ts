// Protocol layouts derived from Apache Kafka 4.0 message definitions.
// See NOTICE and LICENSE-APACHE for attribution. Do not change field order.
import { Reader, Writer } from "./binary.js"

export interface Requests {
  FindCoordinator: { readonly key: string; readonly keyType: number }
  JoinGroup: { readonly groupId: string; readonly sessionTimeoutMs: number; readonly rebalanceTimeoutMs: number; readonly memberId: string; readonly groupInstanceId: string | null; readonly protocolType: string; readonly protocols: ReadonlyArray<{ readonly name: string; readonly metadata: Buffer }> }
  SyncGroup: { readonly groupId: string; readonly generationId: number; readonly memberId: string; readonly groupInstanceId: string | null; readonly assignments: ReadonlyArray<{ readonly memberId: string; readonly assignment: Buffer }> }
  Heartbeat: { readonly groupId: string; readonly generationId: number; readonly memberId: string; readonly groupInstanceId: string | null }
  LeaveGroup: { readonly groupId: string; readonly memberId: string }
  OffsetFetch: { readonly groupId: string; readonly topics: ReadonlyArray<{ readonly name: string; readonly partitionIndexes: ReadonlyArray<number> }> | null }
  OffsetCommit: { readonly groupId: string; readonly generationIdOrMemberEpoch: number; readonly memberId: string; readonly retentionTimeMs: bigint; readonly topics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly committedOffset: bigint; readonly committedMetadata: string | null }> }> }
  ListOffsets: { readonly replicaId: number; readonly isolationLevel: number; readonly topics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly timestamp: bigint }> }> }
  Fetch: { readonly replicaId: number; readonly maxWaitMs: number; readonly minBytes: number; readonly maxBytes: number; readonly isolationLevel: number; readonly sessionId: number; readonly sessionEpoch: number; readonly topics: ReadonlyArray<{ readonly topic: string; readonly partitions: ReadonlyArray<{ readonly partition: number; readonly currentLeaderEpoch: number; readonly fetchOffset: bigint; readonly logStartOffset: bigint; readonly partitionMaxBytes: number }> }>; readonly forgottenTopicsData: ReadonlyArray<{ readonly topic: string; readonly partitions: ReadonlyArray<number> }>; readonly rackId: string }
  InitProducerId: { readonly transactionalId: string | null; readonly transactionTimeoutMs: number }
  AddPartitionsToTxn: { readonly v3AndBelowTransactionalId: string; readonly v3AndBelowProducerId: bigint; readonly v3AndBelowProducerEpoch: number; readonly v3AndBelowTopics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<number> }> }
  AddOffsetsToTxn: { readonly transactionalId: string; readonly producerId: bigint; readonly producerEpoch: number; readonly groupId: string }
  TxnOffsetCommit: { readonly transactionalId: string; readonly groupId: string; readonly producerId: bigint; readonly producerEpoch: number; readonly generationId: number; readonly memberId: string; readonly groupInstanceId: string | null; readonly topics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly committedOffset: bigint; readonly committedLeaderEpoch: number; readonly committedMetadata: string | null }> }> }
  EndTxn: { readonly transactionalId: string; readonly producerId: bigint; readonly producerEpoch: number; readonly committed: boolean }
}

export interface Responses {
  FindCoordinator: { readonly throttleTimeMs: number; readonly errorCode: number; readonly errorMessage: string | null; readonly nodeId: number; readonly host: string; readonly port: number }
  JoinGroup: { readonly throttleTimeMs: number; readonly errorCode: number; readonly generationId: number; readonly protocolName: string; readonly leader: string; readonly memberId: string; readonly members: ReadonlyArray<{ readonly memberId: string; readonly groupInstanceId: string | null; readonly metadata: Buffer }> }
  SyncGroup: { readonly throttleTimeMs: number; readonly errorCode: number; readonly assignment: Buffer }
  Heartbeat: { readonly throttleTimeMs: number; readonly errorCode: number }
  LeaveGroup: { readonly throttleTimeMs: number; readonly errorCode: number }
  OffsetFetch: { readonly throttleTimeMs: number; readonly topics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly committedOffset: bigint; readonly metadata: string | null; readonly errorCode: number }> }>; readonly errorCode: number }
  OffsetCommit: { readonly topics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly errorCode: number }> }> }
  ListOffsets: { readonly throttleTimeMs: number; readonly topics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly errorCode: number; readonly timestamp: bigint; readonly offset: bigint }> }> }
  Fetch: { readonly throttleTimeMs: number; readonly errorCode: number; readonly sessionId: number; readonly responses: ReadonlyArray<{ readonly topic: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly errorCode: number; readonly highWatermark: bigint; readonly lastStableOffset: bigint; readonly logStartOffset: bigint; readonly abortedTransactions: ReadonlyArray<{ readonly producerId: bigint; readonly firstOffset: bigint }> | null; readonly preferredReadReplica: number; readonly records: Buffer | null }> }> }
  InitProducerId: { readonly throttleTimeMs: number; readonly errorCode: number; readonly producerId: bigint; readonly producerEpoch: number }
  AddPartitionsToTxn: { readonly throttleTimeMs: number; readonly resultsByTopicV3AndBelow: ReadonlyArray<{ readonly name: string; readonly resultsByPartition: ReadonlyArray<{ readonly partitionIndex: number; readonly partitionErrorCode: number }> }> }
  AddOffsetsToTxn: { readonly throttleTimeMs: number; readonly errorCode: number }
  TxnOffsetCommit: { readonly throttleTimeMs: number; readonly topics: ReadonlyArray<{ readonly name: string; readonly partitions: ReadonlyArray<{ readonly partitionIndex: number; readonly errorCode: number }> }> }
  EndTxn: { readonly throttleTimeMs: number; readonly errorCode: number }
}

type Field = readonly [string, string, boolean, ReadonlyArray<Field> | null]

const schemas: Record<string, ReadonlyArray<Field>> = {
  "FindCoordinatorRequest": [["key","string",false,null],["keyType","int8",false,null]],
  "FindCoordinatorResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null],["errorMessage","string",true,null],["nodeId","int32",false,null],["host","string",false,null],["port","int32",false,null]],
  "JoinGroupRequest": [["groupId","string",false,null],["sessionTimeoutMs","int32",false,null],["rebalanceTimeoutMs","int32",false,null],["memberId","string",false,null],["groupInstanceId","string",true,null],["protocolType","string",false,null],["protocols","[]struct",false,[["name","string",false,null],["metadata","bytes",false,null]]]],
  "JoinGroupResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null],["generationId","int32",false,null],["protocolName","string",false,null],["leader","string",false,null],["memberId","string",false,null],["members","[]struct",false,[["memberId","string",false,null],["groupInstanceId","string",true,null],["metadata","bytes",false,null]]]],
  "SyncGroupRequest": [["groupId","string",false,null],["generationId","int32",false,null],["memberId","string",false,null],["groupInstanceId","string",true,null],["assignments","[]struct",false,[["memberId","string",false,null],["assignment","bytes",false,null]]]],
  "SyncGroupResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null],["assignment","bytes",false,null]],
  "HeartbeatRequest": [["groupId","string",false,null],["generationId","int32",false,null],["memberId","string",false,null],["groupInstanceId","string",true,null]],
  "HeartbeatResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null]],
  "LeaveGroupRequest": [["groupId","string",false,null],["memberId","string",false,null]],
  "LeaveGroupResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null]],
  "OffsetFetchRequest": [["groupId","string",false,null],["topics","[]struct",true,[["name","string",false,null],["partitionIndexes","[]int32",false,null]]]],
  "OffsetFetchResponse": [["throttleTimeMs","int32",false,null],["topics","[]struct",false,[["name","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["committedOffset","int64",false,null],["metadata","string",true,null],["errorCode","int16",false,null]]]]],["errorCode","int16",false,null]],
  "OffsetCommitRequest": [["groupId","string",false,null],["generationIdOrMemberEpoch","int32",false,null],["memberId","string",false,null],["retentionTimeMs","int64",false,null],["topics","[]struct",false,[["name","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["committedOffset","int64",false,null],["committedMetadata","string",true,null]]]]]],
  "OffsetCommitResponse": [["topics","[]struct",false,[["name","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["errorCode","int16",false,null]]]]]],
  "ListOffsetsRequest": [["replicaId","int32",false,null],["isolationLevel","int8",false,null],["topics","[]struct",false,[["name","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["timestamp","int64",false,null]]]]]],
  "ListOffsetsResponse": [["throttleTimeMs","int32",false,null],["topics","[]struct",false,[["name","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["errorCode","int16",false,null],["timestamp","int64",false,null],["offset","int64",false,null]]]]]],
  "FetchRequest": [["replicaId","int32",false,null],["maxWaitMs","int32",false,null],["minBytes","int32",false,null],["maxBytes","int32",false,null],["isolationLevel","int8",false,null],["sessionId","int32",false,null],["sessionEpoch","int32",false,null],["topics","[]struct",false,[["topic","string",false,null],["partitions","[]struct",false,[["partition","int32",false,null],["currentLeaderEpoch","int32",false,null],["fetchOffset","int64",false,null],["logStartOffset","int64",false,null],["partitionMaxBytes","int32",false,null]]]]],["forgottenTopicsData","[]struct",false,[["topic","string",false,null],["partitions","[]int32",false,null]]],["rackId","string",false,null]],
  "FetchResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null],["sessionId","int32",false,null],["responses","[]struct",false,[["topic","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["errorCode","int16",false,null],["highWatermark","int64",false,null],["lastStableOffset","int64",false,null],["logStartOffset","int64",false,null],["abortedTransactions","[]struct",true,[["producerId","int64",false,null],["firstOffset","int64",false,null]]],["preferredReadReplica","int32",false,null],["records","records",true,null]]]]]],
  "InitProducerIdRequest": [["transactionalId","string",true,null],["transactionTimeoutMs","int32",false,null]],
  "InitProducerIdResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null],["producerId","int64",false,null],["producerEpoch","int16",false,null]],
  "AddPartitionsToTxnRequest": [["v3AndBelowTransactionalId","string",false,null],["v3AndBelowProducerId","int64",false,null],["v3AndBelowProducerEpoch","int16",false,null],["v3AndBelowTopics","[]struct",false,[["name","string",false,null],["partitions","[]int32",false,null]]]],
  "AddPartitionsToTxnResponse": [["throttleTimeMs","int32",false,null],["resultsByTopicV3AndBelow","[]struct",false,[["name","string",false,null],["resultsByPartition","[]struct",false,[["partitionIndex","int32",false,null],["partitionErrorCode","int16",false,null]]]]]],
  "AddOffsetsToTxnRequest": [["transactionalId","string",false,null],["producerId","int64",false,null],["producerEpoch","int16",false,null],["groupId","string",false,null]],
  "AddOffsetsToTxnResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null]],
  "TxnOffsetCommitRequest": [["transactionalId","string",false,null],["groupId","string",false,null],["producerId","int64",false,null],["producerEpoch","int16",false,null],["generationId","int32",false,null],["memberId","string",false,null],["groupInstanceId","string",true,null],["topics","[]struct",false,[["name","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["committedOffset","int64",false,null],["committedLeaderEpoch","int32",false,null],["committedMetadata","string",true,null]]]]]],
  "TxnOffsetCommitResponse": [["throttleTimeMs","int32",false,null],["topics","[]struct",false,[["name","string",false,null],["partitions","[]struct",false,[["partitionIndex","int32",false,null],["errorCode","int16",false,null]]]]]],
  "EndTxnRequest": [["transactionalId","string",false,null],["producerId","int64",false,null],["producerEpoch","int16",false,null],["committed","bool",false,null]],
  "EndTxnResponse": [["throttleTimeMs","int32",false,null],["errorCode","int16",false,null]]
}

export const apis = {"FindCoordinator":[10,1,false],"JoinGroup":[11,5,false],"SyncGroup":[14,3,false],"Heartbeat":[12,3,false],"LeaveGroup":[13,1,false],"OffsetFetch":[9,4,false],"OffsetCommit":[8,2,false],"ListOffsets":[2,2,false],"Fetch":[1,11,false],"InitProducerId":[22,0,false],"AddPartitionsToTxn":[24,0,false],"AddOffsetsToTxn":[25,0,false],"TxnOffsetCommit":[28,3,true],"EndTxn":[26,0,false]} as const
function writeFields(writer: Writer, fields: ReadonlyArray<Field>, value: unknown, flexible: boolean): void {
  const object = value as Record<string, unknown>
  for (const [name, type, nullable, child] of fields) {
    const v = object[name]
    if (type.startsWith("[]")) {
      if (v === null && nullable) { flexible ? writer.uvarint(0) : writer.i32(-1); continue }
      if (!Array.isArray(v)) throw new Error(`Expected array ${name}`)
      flexible ? writer.uvarint(v.length + 1) : writer.i32(v.length)
      for (const entry of v) writeValue(writer, type.slice(2), entry, false, child, flexible)
    } else writeValue(writer, type, v, nullable, child, flexible)
  }
  if (flexible) writer.uvarint(0)
}
function writeValue(w: Writer, type: string, value: unknown, nullable: boolean, child: ReadonlyArray<Field> | null, flexible: boolean): void {
  if (child) { writeFields(w, child, value, flexible); return }
  switch (type) {
    case "int8": w.i8(value as number); return
    case "int16": w.i16(value as number); return
    case "int32": w.i32(value as number); return
    case "int64": w.i64(value as bigint); return
    case "bool": w.i8(value ? 1 : 0); return
    case "string": case "bytes": case "records": {
      if (value === null && nullable) {
        if (flexible) w.uvarint(0)
        else if (type === "string") w.i16(-1)
        else w.i32(-1)
        return
      }
      const b = type === "string" ? Buffer.from(value as string) : value as Buffer
      if (flexible) w.uvarint(b.length + 1).raw(b)
      else if (type === "string") w.string(value as string)
      else w.bytes(b)
      return
    }
    default: throw new Error(`Unknown wire type ${type}`)
  }
}
function readFields(r: Reader, fields: ReadonlyArray<Field>, flexible: boolean): Record<string, unknown> {
  const result: Record<string, unknown> = {}
  for (const [name, type, nullable, child] of fields) {
    if (type.startsWith("[]")) {
      const count = flexible ? r.uvarint() - 1 : r.i32()
      if (count === -1 && nullable) { result[name] = null; continue }
      if (count < 0 || count > r.remaining) throw new Error("Invalid Kafka array length")
      const values: unknown[] = []
      for (let i = 0; i < count; i++) values.push(readValue(r, type.slice(2), false, child, flexible))
      result[name] = values
    } else result[name] = readValue(r, type, nullable, child, flexible)
  }
  if (flexible) r.tags()
  return result
}
function readValue(r: Reader, type: string, nullable: boolean, child: ReadonlyArray<Field> | null, flexible: boolean): unknown {
  if (child) return readFields(r, child, flexible)
  switch (type) {
    case "int8": return r.i8()
    case "int16": return r.i16()
    case "int32": return r.i32()
    case "int64": return r.i64()
    case "bool": { const n = r.i8(); if (n !== 0 && n !== 1) throw new Error("Invalid Kafka boolean"); return n === 1 }
    case "string": case "bytes": case "records": {
      const size = flexible ? r.uvarint() - 1 : type === "string" ? r.i16() : r.i32()
      if (size === -1 && nullable) return null
      const b = r.take(size)
      return type === "string" ? b.toString("utf8") : b
    }
    default: throw new Error(`Unknown wire type ${type}`)
  }
}
export const encode = <K extends keyof Requests>(api: K, value: Requests[K], limit: number): Buffer => {
  const writer = new Writer(limit)
  writeFields(writer, schemas[api + "Request"]!, value, apis[api][2])
  return writer.finish()
}
export const decode = <K extends keyof Responses>(api: K, body: Buffer): Responses[K] => {
  const reader = new Reader(body)
  const value = readFields(reader, schemas[api + "Response"]!, apis[api][2])
  reader.end()
  return value as unknown as Responses[K]
}
