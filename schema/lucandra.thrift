namespace java lucandra.serializers.thrift

typedef i32 int
typedef i64 long


/**
 * Term Information..
 */
struct ThriftTerm {
  1: required string field,             
  2: required string text,  
}

struct DocumentMetadata {
  1: required list<ThriftTerm> terms,
}

