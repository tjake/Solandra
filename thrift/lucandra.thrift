namespace java lucandra.serializers.thrift



/**
 * Term Information..
 */
struct ThriftTerm {
  1: required string field,
  2: optional binary text,
  3: optional bool is_binary,
  4: optional i64 longVal
}

struct DocumentMetadata {
  1: required list<ThriftTerm> terms
}

