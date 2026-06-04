import { apiFetch } from "./apiClient";
import type { SearchResponse, SuggestResponse } from "@/types/search";

const SEARCH_PREFIX = "/api/v1/search";

export const searchService = {
  query: (params: { q: string; size?: number; tagId?: number; page?: number }) => {
    const { q, size = 20, tagId, page = 1 } = params;
    const usp = new URLSearchParams();
    usp.set("q", q);
    if (size) usp.set("size", String(size));
    if (page) usp.set("page", String(page));
    if (tagId) usp.set("tag_id", String(tagId));
    return apiFetch<SearchResponse>(`${SEARCH_PREFIX}?${usp.toString()}`).then((resp) => ({
      ...resp,
      hasMore: typeof resp.total === "number" && typeof resp.page === "number" && typeof resp.pageSize === "number"
        ? resp.page * resp.pageSize < resp.total
        : false
    }));
  },

  suggest: (prefix: string, size = 10) => {
    const usp = new URLSearchParams();
    usp.set("prefix", prefix);
    if (size) usp.set("size", String(size));
    return apiFetch<SuggestResponse>(`${SEARCH_PREFIX}/suggest?${usp.toString()}`);
  }
};
