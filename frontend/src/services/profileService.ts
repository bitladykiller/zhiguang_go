import { apiFetch } from "./apiClient";
import type { ProfileResponse, ProfileUpdateRequest } from "@/types/profile";

const PROFILE_PREFIX = "/api/v1/profiles";

export const profileService = {
  get: (id: number, accessToken?: string) =>
    apiFetch<ProfileResponse>(`${PROFILE_PREFIX}/${id}`, {
      accessToken: accessToken ?? null
    }),

  update: (id: number, payload: ProfileUpdateRequest, accessToken: string) =>
    apiFetch<void>(`${PROFILE_PREFIX}/${id}`, {
      method: "PATCH",
      body: payload,
      accessToken
    }),

  uploadAvatar: (_file: File) =>
    Promise.reject<ProfileResponse>(new Error("当前后端未提供头像上传接口"))
};
