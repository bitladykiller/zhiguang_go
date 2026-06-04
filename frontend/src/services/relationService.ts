import { apiFetch } from "./apiClient";
import type { RelationStatusResponse, RelationCountersResponse } from "@/types/relation";
import type { ProfileResponse } from "@/types/profile";
import { profileService } from "./profileService";

const RELATION_PREFIX = "/api/v1/relations";

export const relationService = {
  follow: (toUserId: number, accessToken: string) =>
    apiFetch<{ success: boolean }>(`${RELATION_PREFIX}/follow`, {
      method: "POST",
      body: { toUserId },
      accessToken
    }),

  unfollow: (toUserId: number, accessToken: string) =>
    apiFetch<{ success: boolean; changed: boolean }>(`${RELATION_PREFIX}/unfollow`, {
      method: "POST",
      body: { toUserId },
      accessToken
    }),

  status: (toUserId: number, accessToken: string) =>
    apiFetch<{ status: string }>(`${RELATION_PREFIX}/status?other_id=${toUserId}`, {
      accessToken
    }).then((resp): RelationStatusResponse => {
      switch (resp.status) {
        case "mutual":
          return { following: true, followedBy: true, mutual: true };
        case "following":
          return { following: true, followedBy: false, mutual: false };
        case "followed":
          return { following: false, followedBy: true, mutual: false };
        default:
          return { following: false, followedBy: false, mutual: false };
      }
    }),

  following: (userId: number, limit = 20, offset = 0, accessToken?: string) => {
    const params = new URLSearchParams({ user_id: String(userId), limit: String(limit), offset: String(offset) });
    return apiFetch<{ data: number[] }>(`${RELATION_PREFIX}/following?${params.toString()}`, {
      accessToken: accessToken ?? null
    }).then(async (resp): Promise<ProfileResponse[]> => {
      const ids = resp.data ?? [];
      const profiles = await Promise.all(ids.map((id) => profileService.get(id, accessToken)));
      return profiles;
    });
  },

  followers: (userId: number, limit = 20, offset = 0, accessToken?: string) => {
    const params = new URLSearchParams({ user_id: String(userId), limit: String(limit), offset: String(offset) });
    return apiFetch<{ data: number[] }>(`${RELATION_PREFIX}/followers?${params.toString()}`, {
      accessToken: accessToken ?? null
    }).then(async (resp): Promise<ProfileResponse[]> => {
      const ids = resp.data ?? [];
      const profiles = await Promise.all(ids.map((id) => profileService.get(id, accessToken)));
      return profiles;
    });
  },

  counters: async (userId: number, accessToken: string): Promise<RelationCountersResponse> => {
    const [followings, followers] = await Promise.all([
      relationService.following(userId, 100, 0, accessToken),
      relationService.followers(userId, 100, 0, accessToken)
    ]);
    return {
      followings: followings.length,
      followers: followers.length,
      posts: 0,
      likedPosts: 0,
      favedPosts: 0
    };
  }
};
