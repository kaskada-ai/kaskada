package utils

import (
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	v1alpha "github.com/kaskada/kaskada-ai/wren/gen/kaskada/kaskada/v1alpha"
)

const redacted_string string = "...redacted..."

// redacts every sensitive field in a protobuf message.
// if the sensitive field is a string, it replaces the string with "redacted"
// otherwise it clears the field
func RedactInPlace(pb proto.Message) {

	m := pb.ProtoReflect()
	m.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		opts := fd.Options().(*descriptorpb.FieldOptions)
		if proto.GetExtension(opts, v1alpha.E_Sensitive).(bool) {
			if fd.Kind() == protoreflect.StringKind {
				if fd.Cardinality() == protoreflect.Repeated {
					// list of strings
					for i := 0; i < v.List().Len(); i++ {
						v.List().Set(i, protoreflect.ValueOfString(redacted_string))
					}
				} else {
					m.Set(fd, protoreflect.ValueOfString(redacted_string))
				}
			} else {
				m.Clear(fd)
			}
		}
		if fd.Kind() == protoreflect.MessageKind {
			if fd.IsList() {
				for i := 0; i < v.List().Len(); i++ {
					RedactInPlace(v.List().Get(i).Message().Interface())
				}
			} else if fd.IsMap() {
				if v.Map().IsValid() {
					if fd.MapValue().Kind() == protoreflect.MessageKind {
						v.Map().Range(func(mapKey protoreflect.MapKey, mapValue protoreflect.Value) bool {
							RedactInPlace(mapValue.Message().Interface())
							return true
						})
					}
				} else {
					// for some reason, map[string]string isn't showing up as a "real" map
					// and therefore '.Range()' can't be used on it, and there is no other
					// way to get the set of keys in the map.
					// for now, just clear the whole map if it is sensitive, instead of
					// just redacting the values.
					map_opts := fd.Options().(*descriptorpb.FieldOptions)
					map_sensitive := proto.GetExtension(map_opts, v1alpha.E_Sensitive).(bool)
					if map_sensitive {
						m.Clear(fd)
					}
				}
			} else {
				RedactInPlace(v.Message().Interface())
			}
		}

		return true
	})
}

// returns a copy of a protobuf message with sensitive fields redacted
// if the sensitive field is a string, it replaces the string with "redacted"
// otherwise it clears the field
func RedactCopy(pb proto.Message) proto.Message {
	pbCopy := proto.Clone(pb)
	RedactInPlace(pbCopy)
	return pbCopy
}
