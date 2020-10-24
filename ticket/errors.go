package ticket

import (
	"errors"
)

var ErrNotFound = errors.New("Entity not found")
var ErrResourceType = errors.New("Resource  type is incorrect")
