import { useEffect, useState, type DependencyList } from "react";

function useAsync<T>(asyncFn: (signal: AbortSignal) => Promise<T>, deps: DependencyList) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const abortController = new AbortController();

    setLoading(true);
    setError(null);

    asyncFn(abortController.signal)
      .then((result) => {
        if (!abortController.signal.aborted) setData(result);
      })
      .catch((err) => {
        if (abortController.signal.aborted) return;
        setError(err instanceof Error ? err : new Error(String(err)));
      })
      .finally(() => {
        if (!abortController.signal.aborted) setLoading(false);
      });

    return () => abortController.abort();
  }, deps);

  return { data, error, loading };
}

export default useAsync;