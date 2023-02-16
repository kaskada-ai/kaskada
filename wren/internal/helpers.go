package internal

import (
	"crypto/md5"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/kaskada/kaskada-ai/wren/ent"
)

// rollback calls to tx.Rollback and wraps the given error
// with the rollback error if occurred.
func rollback(tx *ent.Tx, err error) error {
	if rerr := tx.Rollback(); rerr != nil {
		err = fmt.Errorf("%w: %v", err, rerr)
	}
	return err
}

func GetProtoMd5Sum(protoMessage protoreflect.ProtoMessage) ([]byte, error) {
	marshalledProtoMessage, err := proto.Marshal(protoMessage)
	if err != nil {
		return nil, errors.Wrapf(err, "encoding slice plan")
	}

	md5Hash := md5.New()
	md5Hash.Write(marshalledProtoMessage)
	return md5Hash.Sum(nil), nil
}

func violatesUniqueConstraint(err error) bool {
	if ent.IsConstraintError(err) {
		// postgres error or sqlite error
		if strings.Contains(err.Error(), "violates unique") || strings.Contains(err.Error(), "UNIQUE constraint failed") {
			return true
		}
	}
	return false
}
