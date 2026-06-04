import { apiFetch } from "./apiClient";
import type {
  CreateDraftResponse,
  PresignRequest,
  PresignResponse,
  ConfirmContentRequest,
  UpdateKnowPostRequest,
  FeedResponse,
  KnowpostDetailResponse,
  LikeActionResponse,
  FavActionResponse,
  CounterResponse,
  VisibleScope
} from "@/types/knowpost";

const KNOWPOST_PREFIX = "/api/v1/knowposts";
const STORAGE_PREFIX = "/api/v1/storage";

export const knowpostService = {
  createDraft: () =>
    apiFetch<CreateDraftResponse>(`${KNOWPOST_PREFIX}/draft`, { method: "POST" }),

  presign: (payload: PresignRequest, accessToken: string) =>
    apiFetch<{ uploadUrl: string; objectKey: string; publicUrl: string; expireAt: string }>(`${STORAGE_PREFIX}/presign`, {
      method: "POST",
      body: {
        fileName: payload.fileName,
        contentType: payload.contentType,
        folder: payload.folder
      },
      accessToken
    }).then((resp): PresignResponse => ({
      objectKey: resp.objectKey,
      putUrl: resp.uploadUrl,
      publicUrl: resp.publicUrl,
      headers: {},
      expiresIn: resp.expireAt
    })),

  confirmContent: (id: string, payload: ConfirmContentRequest) =>
    apiFetch<void>(`${KNOWPOST_PREFIX}/${id}/content`, { method: "PUT", body: payload }),

  update: (id: string, payload: UpdateKnowPostRequest) =>
    apiFetch<void>(`${KNOWPOST_PREFIX}/${id}/metadata`, { method: "PUT", body: payload }),

  publish: (id: string) =>
    apiFetch<void>(`${KNOWPOST_PREFIX}/${id}/publish`, { method: "POST" })
  ,
  
  // 设置置顶（需鉴权）
  setTop: (id: string, isTop: boolean, accessToken: string) =>
    apiFetch<void>(`${KNOWPOST_PREFIX}/${id}/top`, {
      method: "PUT",
      body: { isTop },
      accessToken
    })
  ,

  // 设置可见性（需鉴权）
  setVisibility: (id: string, visible: VisibleScope, accessToken: string) =>
    apiFetch<void>(`${KNOWPOST_PREFIX}/${id}/visibility`, {
      method: "PUT",
      body: { visible },
      accessToken
    })
  ,

  // 删除知文（需鉴权）
  remove: (id: string, accessToken: string) =>
    apiFetch<void>(`${KNOWPOST_PREFIX}/${id}`, {
      method: "DELETE",
      accessToken
    })
  ,

  // 获取首页 Feed 列表（公开内容）
  feed: (page = 1, size = 20) =>
    apiFetch<FeedResponse>(`${KNOWPOST_PREFIX}/feed/public?page=${page}&size=${size}`)
  ,

  // 获取我的知文（需鉴权）
  mine: (page = 1, size = 20, accessToken: string) =>
    apiFetch<FeedResponse>(`${KNOWPOST_PREFIX}/feed/mine?page=${page}&size=${size}`, {
      accessToken
    })
  ,

  // 获取知文详情（公开内容无需鉴权；非公开需要作者凭证）
  detail: (id: string, accessToken?: string) =>
    apiFetch<KnowpostDetailResponse>(`${KNOWPOST_PREFIX}/${id}`, {
      accessToken: accessToken ?? null
    })
  ,

  // 生成知文摘要（需鉴权）
  suggestDescription: (id: string, title: string, content: string, accessToken: string) =>
    apiFetch<{ description: string }>(`${KNOWPOST_PREFIX}/${id}/description/suggest`, {
      method: "POST",
      body: { title, content },
      accessToken
    })
  ,

  // 点赞/取消点赞（需鉴权）
  like: (entityId: string, accessToken: string, entityType: string = "knowpost") =>
    apiFetch<{ changed: boolean; success: boolean }>(`/api/v1/counter/like`, {
      method: "POST",
      body: { entityType, entityId },
      accessToken
    }).then((resp): LikeActionResponse => ({ changed: resp.changed, liked: true }))
  ,
  unlike: (entityId: string, accessToken: string, entityType: string = "knowpost") =>
    apiFetch<{ changed: boolean; success: boolean }>(`/api/v1/counter/unlike`, {
      method: "POST",
      body: { entityType, entityId },
      accessToken
    }).then((resp): LikeActionResponse => ({ changed: resp.changed, liked: false }))
  ,

  // 收藏/取消收藏（需鉴权）
  fav: (entityId: string, accessToken: string, entityType: string = "knowpost") =>
    apiFetch<{ changed: boolean; success: boolean }>(`/api/v1/counter/fav`, {
      method: "POST",
      body: { entityType, entityId },
      accessToken
    }).then((resp): FavActionResponse => ({ changed: resp.changed, faved: true }))
  ,
  unfav: (entityId: string, accessToken: string, entityType: string = "knowpost") =>
    apiFetch<{ changed: boolean; success: boolean }>(`/api/v1/counter/unfav`, {
      method: "POST",
      body: { entityType, entityId },
      accessToken
    }).then((resp): FavActionResponse => ({ changed: resp.changed, faved: false }))
  ,

  // 获取计数（需鉴权）
  counters: (entityId: string, accessToken: string, entityType: string = "knowpost") =>
    apiFetch<{ data: Record<string, number> }>(`/api/v1/counter/counts?entity_type=${entityType}&entity_id=${entityId}&metrics=like,fav`, {
      accessToken
    }).then((resp): CounterResponse => ({
      entityType,
      entityId,
      counts: {
        like: resp.data?.like ?? 0,
        fav: resp.data?.fav ?? 0
      }
    })),

  status: (entityId: string, accessToken: string, entityType: string = "knowpost") =>
    apiFetch<{ isLiked: boolean; isFaved: boolean }>(`/api/v1/counter/status?entity_type=${entityType}&entity_id=${entityId}`, {
      accessToken
    })
};

/**
 * 直传到预签名 URL。注意：S3/OSS 会在响应头返回 ETag。
 */
export async function uploadToPresigned(putUrl: string, headers: Record<string, string>, file: File) {
  const resp = await fetch(putUrl, {
    method: "PUT",
    headers,
    body: file,
    // 跨域上传通常不需要携带凭据
    credentials: "omit"
  });
  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(text || `上传失败：${resp.status}`);
  }
  // ETag 常带双引号
  const etag = resp.headers.get("ETag") || resp.headers.get("etag") || "";
  return { etag };
}

export async function computeSha256(file: File) {
  const buf = await file.arrayBuffer();
  const digest = await crypto.subtle.digest("SHA-256", buf);
  const hex = Array.from(new Uint8Array(digest)).map(b => b.toString(16).padStart(2, "0")).join("");
  return hex;
}
