package profile

import (
	"context"
	"errors"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/pkg/errcode"
)

// mockRepo 实现 Repo 接口，用于 Service 层单元测试
type mockRepo struct {
	user *UserProfile
	err  error
}

func (r *mockRepo) FindByID(_ context.Context, id uint64) (*UserProfile, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.user, nil
}

func (r *mockRepo) Update(_ context.Context, id uint64, req *ProfilePatchRequest) error {
	return r.err
}

func (r *mockRepo) WithDB(_ sqlx.ExtContext) *Repository {
	return nil
}

func helpPtr(s string) *string { return &s }

// --- GetProfile ---

func TestSvcGetProfile_Success(t *testing.T) {
	mock := &mockRepo{user: &UserProfile{ID: 1, Nickname: "alice"}}
	svc := NewProfileService(mock)

	user, appErr := svc.GetProfile(context.Background(), 1)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
	if user.ID != 1 || user.Nickname != "alice" {
		t.Errorf("unexpected user: %+v", user)
	}
}

func TestSvcGetProfile_NotFound(t *testing.T) {
	mock := &mockRepo{err: errors.New("not found")}
	svc := NewProfileService(mock)

	_, appErr := svc.GetProfile(context.Background(), 999)
	if appErr == nil {
		t.Fatal("expected error")
	}
	if appErr.Code != errcode.CodeNotFound {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeNotFound)
	}
}

// --- UpdateProfile ---

func TestSvcUpdateProfile_Success(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("bob")}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_Forbidden(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)
	req := &ProfilePatchRequest{Nickname: helpPtr("hacker")}
	appErr := svc.UpdateProfile(context.Background(), 1, 2, req)
	if appErr == nil {
		t.Fatal("expected error")
	}
	if appErr.Code != errcode.CodeForbidden {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeForbidden)
	}
}

func TestSvcUpdateProfile_NoFields(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)
	req := &ProfilePatchRequest{}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr == nil {
		t.Fatal("expected error")
	}
	if appErr.Code != errcode.CodeBadRequest {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeBadRequest)
	}
}

func TestSvcUpdateProfile_AllFieldsNil(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)
	req := &ProfilePatchRequest{}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr == nil {
		t.Fatal("expected error for all-nil fields")
	}
}

func TestSvcUpdateProfile_NicknameOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("new-name")}
	appErr := svc.UpdateProfile(context.Background(), 10, 10, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_AvatarOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Avatar: helpPtr("https://example.com/avatar.png")}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_BioOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Bio: helpPtr("hello world")}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_GenderOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Gender: helpPtr("male")}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_SchoolOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{School: helpPtr("MIT")}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_TagsJsonOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	tags := `["go","rust"]`
	req := &ProfilePatchRequest{TagsJson: &tags}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_BirthdayOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	bday := "2000-01-01"
	req := &ProfilePatchRequest{Birthday: &bday}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

func TestSvcUpdateProfile_RepoError(t *testing.T) {
	mock := &mockRepo{err: errors.New("db error")}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("test")}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr == nil {
		t.Fatal("expected error")
	}
	if appErr.Code != errcode.CodeInternalError {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeInternalError)
	}
}

func TestSvcUpdateProfile_SameUser(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("test")}
	appErr := svc.UpdateProfile(context.Background(), 1, 1, req)
	if appErr != nil {
		t.Fatalf("unexpected error: %v", appErr)
	}
}

// --- NewProfileService ---

func TestNewProfileService(t *testing.T) {
	repo := &Repository{}
	svc := NewProfileService(repo).(*Service)
	if svc.repo != repo {
		t.Error("repo not set")
	}
}

func TestServiceImplementsInterface(t *testing.T) {
	var _ ProfileServicer = (*Service)(nil)
}
