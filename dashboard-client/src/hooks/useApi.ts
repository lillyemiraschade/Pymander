/* ── Generic API query hook wrapping @tanstack/react-query ── */

import { useQuery, type UseQueryOptions, type UseQueryResult } from "@tanstack/react-query";

export function useApi<T>(
  key: readonly unknown[],
  fetcher: () => Promise<T>,
  options?: Omit<UseQueryOptions<T, Error, T, readonly unknown[]>, "queryKey" | "queryFn">,
): UseQueryResult<T, Error> {
  return useQuery<T, Error, T, readonly unknown[]>({
    queryKey: key,
    queryFn: fetcher,
    ...options,
  });
}
