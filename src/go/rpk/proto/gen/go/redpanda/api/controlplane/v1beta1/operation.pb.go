// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.32.0
// 	protoc        (unknown)
// source: redpanda/api/controlplane/v1beta1/operation.proto

package controlplanev1beta1

import (
	_ "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	_ "github.com/grpc-ecosystem/grpc-gateway/v2/protoc-gen-openapiv2/options"
	_ "github.com/redpanda-data/redpanda/src/go/rpk/proto/gen/go/redpanda/api/common/v1alpha1"
	_ "google.golang.org/genproto/googleapis/api/annotations"
	status "google.golang.org/genproto/googleapis/rpc/status"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	anypb "google.golang.org/protobuf/types/known/anypb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// describes if the operation has finished, or is still in progress. Only if done is true, either error or result become available.
type Operation_State int32

const (
	Operation_STATE_UNSPECIFIED Operation_State = 0
	Operation_STATE_IN_PROGRESS Operation_State = 1
	Operation_STATE_COMPLETED   Operation_State = 2
	Operation_STATE_FAILED      Operation_State = 3
)

// Enum value maps for Operation_State.
var (
	Operation_State_name = map[int32]string{
		0: "STATE_UNSPECIFIED",
		1: "STATE_IN_PROGRESS",
		2: "STATE_COMPLETED",
		3: "STATE_FAILED",
	}
	Operation_State_value = map[string]int32{
		"STATE_UNSPECIFIED": 0,
		"STATE_IN_PROGRESS": 1,
		"STATE_COMPLETED":   2,
		"STATE_FAILED":      3,
	}
)

func (x Operation_State) Enum() *Operation_State {
	p := new(Operation_State)
	*p = x
	return p
}

func (x Operation_State) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Operation_State) Descriptor() protoreflect.EnumDescriptor {
	return file_redpanda_api_controlplane_v1beta1_operation_proto_enumTypes[0].Descriptor()
}

func (Operation_State) Type() protoreflect.EnumType {
	return &file_redpanda_api_controlplane_v1beta1_operation_proto_enumTypes[0]
}

func (x Operation_State) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Operation_State.Descriptor instead.
func (Operation_State) EnumDescriptor() ([]byte, []int) {
	return file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescGZIP(), []int{0, 0}
}

// Operation describes a long running operation
type Operation struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// ID of the operation.
	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
	// Metadata of the long-running operation.
	Metadata *anypb.Any      `protobuf:"bytes,2,opt,name=metadata,proto3" json:"metadata,omitempty"`
	State    Operation_State `protobuf:"varint,3,opt,name=state,proto3,enum=redpanda.api.controlplane.v1beta1.Operation_State" json:"state,omitempty"`
	// result only available when the Operation is done, it can be either and error or the result of the operation
	//
	// Types that are assignable to Result:
	//
	//	*Operation_Error
	//	*Operation_Response
	Result isOperation_Result `protobuf_oneof:"result"`
	// Timestamp when the operation has started.
	StartedAt *timestamppb.Timestamp `protobuf:"bytes,6,opt,name=started_at,json=startedAt,proto3" json:"started_at,omitempty"`
	// Timestamp when the operation has finished.
	FinishedAt *timestamppb.Timestamp `protobuf:"bytes,7,opt,name=finished_at,json=finishedAt,proto3" json:"finished_at,omitempty"`
}

func (x *Operation) Reset() {
	*x = Operation{}
	if protoimpl.UnsafeEnabled {
		mi := &file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Operation) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Operation) ProtoMessage() {}

func (x *Operation) ProtoReflect() protoreflect.Message {
	mi := &file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Operation.ProtoReflect.Descriptor instead.
func (*Operation) Descriptor() ([]byte, []int) {
	return file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescGZIP(), []int{0}
}

func (x *Operation) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

func (x *Operation) GetMetadata() *anypb.Any {
	if x != nil {
		return x.Metadata
	}
	return nil
}

func (x *Operation) GetState() Operation_State {
	if x != nil {
		return x.State
	}
	return Operation_STATE_UNSPECIFIED
}

func (m *Operation) GetResult() isOperation_Result {
	if m != nil {
		return m.Result
	}
	return nil
}

func (x *Operation) GetError() *status.Status {
	if x, ok := x.GetResult().(*Operation_Error); ok {
		return x.Error
	}
	return nil
}

func (x *Operation) GetResponse() *anypb.Any {
	if x, ok := x.GetResult().(*Operation_Response); ok {
		return x.Response
	}
	return nil
}

func (x *Operation) GetStartedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.StartedAt
	}
	return nil
}

func (x *Operation) GetFinishedAt() *timestamppb.Timestamp {
	if x != nil {
		return x.FinishedAt
	}
	return nil
}

type isOperation_Result interface {
	isOperation_Result()
}

type Operation_Error struct {
	Error *status.Status `protobuf:"bytes,4,opt,name=error,proto3,oneof"`
}

type Operation_Response struct {
	Response *anypb.Any `protobuf:"bytes,5,opt,name=response,proto3,oneof"`
}

func (*Operation_Error) isOperation_Result() {}

func (*Operation_Response) isOperation_Result() {}

// GetOperationRequest is the request of GetOperation
type GetOperationRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Id string `protobuf:"bytes,1,opt,name=id,proto3" json:"id,omitempty"`
}

func (x *GetOperationRequest) Reset() {
	*x = GetOperationRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *GetOperationRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetOperationRequest) ProtoMessage() {}

func (x *GetOperationRequest) ProtoReflect() protoreflect.Message {
	mi := &file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetOperationRequest.ProtoReflect.Descriptor instead.
func (*GetOperationRequest) Descriptor() ([]byte, []int) {
	return file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescGZIP(), []int{1}
}

func (x *GetOperationRequest) GetId() string {
	if x != nil {
		return x.Id
	}
	return ""
}

var File_redpanda_api_controlplane_v1beta1_operation_proto protoreflect.FileDescriptor

var file_redpanda_api_controlplane_v1beta1_operation_proto_rawDesc = []byte{
	0x0a, 0x31, 0x72, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63,
	0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65,
	0x74, 0x61, 0x31, 0x2f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x21, 0x72, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x2e, 0x61, 0x70,
	0x69, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x1a, 0x1b, 0x62, 0x75, 0x66, 0x2f, 0x76, 0x61, 0x6c, 0x69,
	0x64, 0x61, 0x74, 0x65, 0x2f, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x1a, 0x1c, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f,
	0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x5f, 0x62, 0x65, 0x68, 0x61, 0x76, 0x69, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x62, 0x75, 0x66, 0x2f, 0x61, 0x6e, 0x79, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x1f, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x74,
	0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x17,
	0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x72, 0x70, 0x63, 0x2f, 0x73, 0x74, 0x61, 0x74, 0x75,
	0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x2d,
	0x67, 0x65, 0x6e, 0x2d, 0x6f, 0x70, 0x65, 0x6e, 0x61, 0x70, 0x69, 0x76, 0x32, 0x2f, 0x6f, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x61, 0x6e, 0x6e, 0x6f, 0x74, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x73, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x2a, 0x72, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64,
	0x61, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6d, 0x6d, 0x6f, 0x6e, 0x2f, 0x76, 0x31, 0x61,
	0x6c, 0x70, 0x68, 0x61, 0x31, 0x2f, 0x6f, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x22, 0xe1, 0x04, 0x0a, 0x09, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f,
	0x6e, 0x12, 0x2d, 0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x1d, 0xe0,
	0x41, 0x05, 0xe0, 0x41, 0x03, 0xba, 0x48, 0x14, 0x72, 0x12, 0x32, 0x0d, 0x5e, 0x5b, 0x61, 0x2d,
	0x76, 0x30, 0x2d, 0x39, 0x5d, 0x7b, 0x32, 0x30, 0x7d, 0x98, 0x01, 0x14, 0x52, 0x02, 0x69, 0x64,
	0x12, 0x30, 0x0a, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0x18, 0x02, 0x20, 0x01,
	0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79, 0x52, 0x08, 0x6d, 0x65, 0x74, 0x61, 0x64, 0x61,
	0x74, 0x61, 0x12, 0x48, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x32, 0x2e, 0x72, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x2e, 0x61, 0x70, 0x69,
	0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x76, 0x31,
	0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x53, 0x74, 0x61, 0x74, 0x65, 0x52, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x12, 0x2a, 0x0a, 0x05,
	0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x72, 0x70, 0x63, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x48,
	0x00, 0x52, 0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x5b, 0x0a, 0x08, 0x72, 0x65, 0x73, 0x70,
	0x6f, 0x6e, 0x73, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x41, 0x6e, 0x79,
	0x42, 0x27, 0x92, 0x41, 0x24, 0x2a, 0x22, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x20,
	0x6f, 0x66, 0x20, 0x74, 0x68, 0x65, 0x20, 0x75, 0x6e, 0x64, 0x65, 0x72, 0x6c, 0x79, 0x69, 0x6e,
	0x67, 0x20, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x52, 0x08, 0x72, 0x65, 0x73,
	0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x39, 0x0a, 0x0a, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64,
	0x5f, 0x61, 0x74, 0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x52, 0x09, 0x73, 0x74, 0x61, 0x72, 0x74, 0x65, 0x64, 0x41, 0x74,
	0x12, 0x3b, 0x0a, 0x0b, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x5f, 0x61, 0x74, 0x18,
	0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x52, 0x0a, 0x66, 0x69, 0x6e, 0x69, 0x73, 0x68, 0x65, 0x64, 0x41, 0x74, 0x22, 0x5c, 0x0a,
	0x05, 0x53, 0x74, 0x61, 0x74, 0x65, 0x12, 0x15, 0x0a, 0x11, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f,
	0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x15, 0x0a,
	0x11, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f, 0x49, 0x4e, 0x5f, 0x50, 0x52, 0x4f, 0x47, 0x52, 0x45,
	0x53, 0x53, 0x10, 0x01, 0x12, 0x13, 0x0a, 0x0f, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f, 0x43, 0x4f,
	0x4d, 0x50, 0x4c, 0x45, 0x54, 0x45, 0x44, 0x10, 0x02, 0x12, 0x10, 0x0a, 0x0c, 0x53, 0x54, 0x41,
	0x54, 0x45, 0x5f, 0x46, 0x41, 0x49, 0x4c, 0x45, 0x44, 0x10, 0x03, 0x3a, 0x40, 0x92, 0x41, 0x3d,
	0x0a, 0x3b, 0x2a, 0x09, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x32, 0x2c, 0x4f,
	0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x62,
	0x65, 0x73, 0x20, 0x61, 0x20, 0x6c, 0x6f, 0x6e, 0x67, 0x20, 0x72, 0x75, 0x6e, 0x6e, 0x69, 0x6e,
	0x67, 0x20, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x40, 0x01, 0x42, 0x08, 0x0a,
	0x06, 0x72, 0x65, 0x73, 0x75, 0x6c, 0x74, 0x22, 0x25, 0x0a, 0x13, 0x47, 0x65, 0x74, 0x4f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0e,
	0x0a, 0x02, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x02, 0x69, 0x64, 0x32, 0xe3,
	0x04, 0x0a, 0x10, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x53, 0x65, 0x72, 0x76,
	0x69, 0x63, 0x65, 0x12, 0xce, 0x04, 0x0a, 0x0c, 0x47, 0x65, 0x74, 0x4f, 0x70, 0x65, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x12, 0x36, 0x2e, 0x72, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x2e,
	0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65,
	0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x2e, 0x47, 0x65, 0x74, 0x4f, 0x70, 0x65, 0x72,
	0x61, 0x74, 0x69, 0x6f, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x1a, 0x2c, 0x2e, 0x72,
	0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6e, 0x74,
	0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31,
	0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0xd7, 0x03, 0x92, 0x41, 0x99,
	0x03, 0x12, 0x0d, 0x47, 0x65, 0x74, 0x20, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e,
	0x1a, 0x5b, 0x47, 0x65, 0x74, 0x20, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2e,
	0x20, 0x54, 0x68, 0x69, 0x73, 0x20, 0x69, 0x73, 0x20, 0x61, 0x20, 0x67, 0x65, 0x6e, 0x65, 0x72,
	0x69, 0x63, 0x20, 0x65, 0x6e, 0x64, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x2c, 0x20, 0x73, 0x75, 0x70,
	0x70, 0x6f, 0x72, 0x74, 0x69, 0x6e, 0x67, 0x20, 0x72, 0x65, 0x74, 0x72, 0x69, 0x65, 0x76, 0x61,
	0x6c, 0x20, 0x6f, 0x66, 0x20, 0x61, 0x6c, 0x6c, 0x20, 0x6b, 0x69, 0x6e, 0x64, 0x73, 0x20, 0x6f,
	0x66, 0x20, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2e, 0x4a, 0x3d, 0x0a,
	0x03, 0x32, 0x30, 0x30, 0x12, 0x36, 0x0a, 0x02, 0x4f, 0x4b, 0x12, 0x30, 0x0a, 0x2e, 0x1a, 0x2c,
	0x2e, 0x72, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f,
	0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x31, 0x2e, 0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4a, 0x95, 0x01, 0x0a,
	0x03, 0x34, 0x30, 0x34, 0x12, 0x8d, 0x01, 0x0a, 0x09, 0x4e, 0x6f, 0x74, 0x20, 0x46, 0x6f, 0x75,
	0x6e, 0x64, 0x12, 0x16, 0x0a, 0x14, 0x1a, 0x12, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x72, 0x70, 0x63, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x22, 0x68, 0x0a, 0x26, 0x4f, 0x70,
	0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x20, 0x77, 0x69, 0x74, 0x68, 0x20, 0x67, 0x69, 0x76,
	0x65, 0x6e, 0x20, 0x49, 0x44, 0x20, 0x64, 0x6f, 0x65, 0x73, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x65,
	0x78, 0x69, 0x73, 0x74, 0x12, 0x3e, 0x7b, 0x22, 0x63, 0x6f, 0x64, 0x65, 0x22, 0x3a, 0x36, 0x2c,
	0x22, 0x6d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0x3a, 0x22, 0x4f, 0x70, 0x65, 0x72, 0x61,
	0x74, 0x69, 0x6f, 0x6e, 0x20, 0x77, 0x69, 0x74, 0x68, 0x20, 0x67, 0x69, 0x76, 0x65, 0x6e, 0x20,
	0x49, 0x44, 0x20, 0x64, 0x6f, 0x65, 0x73, 0x20, 0x6e, 0x6f, 0x74, 0x20, 0x65, 0x78, 0x69, 0x73,
	0x74, 0x2e, 0x22, 0x7d, 0x4a, 0x54, 0x0a, 0x03, 0x35, 0x30, 0x30, 0x12, 0x4d, 0x0a, 0x33, 0x49,
	0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x20, 0x53, 0x65, 0x72, 0x76, 0x65, 0x72, 0x20, 0x45,
	0x72, 0x72, 0x6f, 0x72, 0x2e, 0x20, 0x50, 0x6c, 0x65, 0x61, 0x73, 0x65, 0x20, 0x72, 0x65, 0x61,
	0x63, 0x68, 0x20, 0x6f, 0x75, 0x74, 0x20, 0x74, 0x6f, 0x20, 0x73, 0x75, 0x70, 0x70, 0x6f, 0x72,
	0x74, 0x2e, 0x12, 0x16, 0x0a, 0x14, 0x1a, 0x12, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e,
	0x72, 0x70, 0x63, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0xb2, 0xbf, 0x07, 0x16, 0x72, 0x65,
	0x61, 0x64, 0x3a, 0x6f, 0x72, 0x67, 0x61, 0x6e, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x2d,
	0x69, 0x6e, 0x66, 0x6f, 0x82, 0xd3, 0xe4, 0x93, 0x02, 0x1a, 0x12, 0x18, 0x2f, 0x76, 0x31, 0x62,
	0x65, 0x74, 0x61, 0x31, 0x2f, 0x6f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f,
	0x7b, 0x69, 0x64, 0x7d, 0x42, 0xcf, 0x02, 0x0a, 0x25, 0x63, 0x6f, 0x6d, 0x2e, 0x72, 0x65, 0x64,
	0x70, 0x61, 0x6e, 0x64, 0x61, 0x2e, 0x61, 0x70, 0x69, 0x2e, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f,
	0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x42, 0x0e,
	0x4f, 0x70, 0x65, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01,
	0x5a, 0x6f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x72, 0x65, 0x64,
	0x70, 0x61, 0x6e, 0x64, 0x61, 0x2d, 0x64, 0x61, 0x74, 0x61, 0x2f, 0x72, 0x65, 0x64, 0x70, 0x61,
	0x6e, 0x64, 0x61, 0x2f, 0x73, 0x72, 0x63, 0x2f, 0x67, 0x6f, 0x2f, 0x72, 0x70, 0x6b, 0x2f, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2f, 0x67, 0x65, 0x6e, 0x2f, 0x67, 0x6f, 0x2f, 0x72, 0x65, 0x64, 0x70,
	0x61, 0x6e, 0x64, 0x61, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x63, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c,
	0x70, 0x6c, 0x61, 0x6e, 0x65, 0x2f, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x3b, 0x63, 0x6f,
	0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x31, 0xa2, 0x02, 0x03, 0x52, 0x41, 0x43, 0xaa, 0x02, 0x21, 0x52, 0x65, 0x64, 0x70, 0x61, 0x6e,
	0x64, 0x61, 0x2e, 0x41, 0x70, 0x69, 0x2e, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c,
	0x61, 0x6e, 0x65, 0x2e, 0x56, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0xca, 0x02, 0x21, 0x52, 0x65,
	0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x5c, 0x41, 0x70, 0x69, 0x5c, 0x43, 0x6f, 0x6e, 0x74, 0x72,
	0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x5c, 0x56, 0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0xe2,
	0x02, 0x2d, 0x52, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x5c, 0x41, 0x70, 0x69, 0x5c, 0x43,
	0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x5c, 0x56, 0x31, 0x62, 0x65,
	0x74, 0x61, 0x31, 0x5c, 0x47, 0x50, 0x42, 0x4d, 0x65, 0x74, 0x61, 0x64, 0x61, 0x74, 0x61, 0xea,
	0x02, 0x24, 0x52, 0x65, 0x64, 0x70, 0x61, 0x6e, 0x64, 0x61, 0x3a, 0x3a, 0x41, 0x70, 0x69, 0x3a,
	0x3a, 0x43, 0x6f, 0x6e, 0x74, 0x72, 0x6f, 0x6c, 0x70, 0x6c, 0x61, 0x6e, 0x65, 0x3a, 0x3a, 0x56,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescOnce sync.Once
	file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescData = file_redpanda_api_controlplane_v1beta1_operation_proto_rawDesc
)

func file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescGZIP() []byte {
	file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescOnce.Do(func() {
		file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescData = protoimpl.X.CompressGZIP(file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescData)
	})
	return file_redpanda_api_controlplane_v1beta1_operation_proto_rawDescData
}

var file_redpanda_api_controlplane_v1beta1_operation_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_redpanda_api_controlplane_v1beta1_operation_proto_goTypes = []interface{}{
	(Operation_State)(0),          // 0: redpanda.api.controlplane.v1beta1.Operation.State
	(*Operation)(nil),             // 1: redpanda.api.controlplane.v1beta1.Operation
	(*GetOperationRequest)(nil),   // 2: redpanda.api.controlplane.v1beta1.GetOperationRequest
	(*anypb.Any)(nil),             // 3: google.protobuf.Any
	(*status.Status)(nil),         // 4: google.rpc.Status
	(*timestamppb.Timestamp)(nil), // 5: google.protobuf.Timestamp
}
var file_redpanda_api_controlplane_v1beta1_operation_proto_depIdxs = []int32{
	3, // 0: redpanda.api.controlplane.v1beta1.Operation.metadata:type_name -> google.protobuf.Any
	0, // 1: redpanda.api.controlplane.v1beta1.Operation.state:type_name -> redpanda.api.controlplane.v1beta1.Operation.State
	4, // 2: redpanda.api.controlplane.v1beta1.Operation.error:type_name -> google.rpc.Status
	3, // 3: redpanda.api.controlplane.v1beta1.Operation.response:type_name -> google.protobuf.Any
	5, // 4: redpanda.api.controlplane.v1beta1.Operation.started_at:type_name -> google.protobuf.Timestamp
	5, // 5: redpanda.api.controlplane.v1beta1.Operation.finished_at:type_name -> google.protobuf.Timestamp
	2, // 6: redpanda.api.controlplane.v1beta1.OperationService.GetOperation:input_type -> redpanda.api.controlplane.v1beta1.GetOperationRequest
	1, // 7: redpanda.api.controlplane.v1beta1.OperationService.GetOperation:output_type -> redpanda.api.controlplane.v1beta1.Operation
	7, // [7:8] is the sub-list for method output_type
	6, // [6:7] is the sub-list for method input_type
	6, // [6:6] is the sub-list for extension type_name
	6, // [6:6] is the sub-list for extension extendee
	0, // [0:6] is the sub-list for field type_name
}

func init() { file_redpanda_api_controlplane_v1beta1_operation_proto_init() }
func file_redpanda_api_controlplane_v1beta1_operation_proto_init() {
	if File_redpanda_api_controlplane_v1beta1_operation_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Operation); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*GetOperationRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*Operation_Error)(nil),
		(*Operation_Response)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_redpanda_api_controlplane_v1beta1_operation_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   1,
		},
		GoTypes:           file_redpanda_api_controlplane_v1beta1_operation_proto_goTypes,
		DependencyIndexes: file_redpanda_api_controlplane_v1beta1_operation_proto_depIdxs,
		EnumInfos:         file_redpanda_api_controlplane_v1beta1_operation_proto_enumTypes,
		MessageInfos:      file_redpanda_api_controlplane_v1beta1_operation_proto_msgTypes,
	}.Build()
	File_redpanda_api_controlplane_v1beta1_operation_proto = out.File
	file_redpanda_api_controlplane_v1beta1_operation_proto_rawDesc = nil
	file_redpanda_api_controlplane_v1beta1_operation_proto_goTypes = nil
	file_redpanda_api_controlplane_v1beta1_operation_proto_depIdxs = nil
}
