package ticket

import (
	"errors"
)

var ErrNotFound = errors.New("entity not found")
var ErrResourceType = errors.New("resource  type is incorrect")
