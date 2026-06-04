import type { FeedItem } from "@/types/knowpost";

export type SearchResponse = {
  items: FeedItem[];
  hasMore: boolean;
  total?: number;
  page?: number;
  pageSize?: number;
};

export type SuggestResponse = {
  items: string[];
};
