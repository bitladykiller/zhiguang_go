package profile

import (
	"context"
	"errors"
	"testing"

	"github.com/jmoiron/sqlx"
	"github.com/zhiguang/app/pkg/errcode"
)

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

func TestSvcGetProfile_Success(t *testing.T) {
	mock := &mockRepo{user: &UserProfile{ID: 1, Nickname: "alice"}}
	svc := NewProfileService(mock)

	user, err := svc.GetProfile(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if user.ID != 1 || user.Nickname != "alice" {
		t.Errorf("unexpected user: %+v", user)
	}
}

func TestSvcGetProfile_NotFound(t *testing.T) {
	mock := &mockRepo{err: errors.New("not found")}
	svc := NewProfileService(mock)

	_, err := svc.GetProfile(context.Background(), 999)
	if err == nil {
		t.Fatal("expected error")
	}
	var appErr *errcode.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected AppError, got %T", err)
	}
	if appErr.Code != errcode.CodeNotFound {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeNotFound)
	}
}

func TestSvcUpdateProfile_Success(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("bob")}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_Forbidden(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)
	req := &ProfilePatchRequest{Nickname: helpPtr("hacker")}
	err := svc.UpdateProfile(context.Background(), 1, 2, req)
	if err == nil {
		t.Fatal("expected error")
	}
	var appErr *errcode.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected AppError, got %T", err)
	}
	if appErr.Code != errcode.CodeForbidden {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeForbidden)
	}
}

func TestSvcUpdateProfile_NoFields(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)
	req := &ProfilePatchRequest{}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err == nil {
		t.Fatal("expected error")
	}
	var appErr *errcode.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected AppError, got %T", err)
	}
	if appErr.Code != errcode.CodeBadRequest {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeBadRequest)
	}
}

func TestSvcUpdateProfile_AllFieldsNil(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)
	req := &ProfilePatchRequest{}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err == nil {
		t.Fatal("expected error for all-nil fields")
	}
}

func TestSvcUpdateProfile_NicknameOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("new-name")}
	err := svc.UpdateProfile(context.Background(), 10, 10, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_AvatarOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Avatar: helpPtr("https://example.com/avatar.png")}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_BioOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Bio: helpPtr("hello world")}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_GenderOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Gender: helpPtr("male")}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_SchoolOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{School: helpPtr("MIT")}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_TagsJsonOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	tags := `["go","rust"]`
	req := &ProfilePatchRequest{TagsJson: &tags}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_BirthdayOnly(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	bday := "2000-01-01"
	req := &ProfilePatchRequest{Birthday: &bday}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSvcUpdateProfile_RepoError(t *testing.T) {
	mock := &mockRepo{err: errors.New("db error")}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("test")}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err == nil {
		t.Fatal("expected error")
	}
	var appErr *errcode.AppError
	if !errors.As(err, &appErr) {
		t.Fatalf("expected AppError, got %T", err)
	}
	if appErr.Code != errcode.CodeInternalError {
		t.Errorf("code = %d, want %d", appErr.Code, errcode.CodeInternalError)
	}
}

func TestSvcUpdateProfile_SameUser(t *testing.T) {
	mock := &mockRepo{}
	svc := NewProfileService(mock)

	req := &ProfilePatchRequest{Nickname: helpPtr("test")}
	err := svc.UpdateProfile(context.Background(), 1, 1, req)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestNewProfileService(t *testing.T) {
	repo := &Repository{}
	svc := NewProfileService(repo)
	if svc.repo != repo {
		t.Error("repo not set")
	}
}

func TestServiceImplementsInterface(t *testing.T) {
	var _ ProfileServiceInterface = (*Service)(nil)
}