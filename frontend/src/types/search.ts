import type { FeedItem } from "@/types/knowpost";

export type SearchResponse = {
  items: FeedItem[];
  hasMore: boolean;
  nextAfter?: string | null;
};

export type SuggestResponse = {
  items: string[];
};
