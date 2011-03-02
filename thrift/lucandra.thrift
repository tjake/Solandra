namespace java lucandra.serializers.thrift

/**
 * Term Information..
 */
struct ThriftTerm {
  1: required string field,
  2: required string text
}

struct DocumentMetadata {
  1: required list<ThriftTerm> terms
}

