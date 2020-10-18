package routine

import "github.com/PatchouliG/lsm-db/record"

// get,delete,put readRequest
// handle by work routine
const getRequest = "getRequest"
const deleteRequest = "deleteRequest"
const putRequest = "putRequest"

type readRequest struct {
	key record.Key
}
type writeRequest struct {
	r record.Record
}
