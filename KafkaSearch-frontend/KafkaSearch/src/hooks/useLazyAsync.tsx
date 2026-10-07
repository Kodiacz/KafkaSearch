import { useCallback, useRef, useState } from "react";

function useLazyAsync<T>() {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);

  const run = useCallback(async (asyncFn: (signal: AbortSignal) => Promise<T>) => {
    abortControllerRef.current?.abort(); // cancel a previous still-in-flight click
    const abortController = new AbortController();
    abortControllerRef.current = abortController;

    setLoading(true);
    setError(null);

    try {
      const result = await asyncFn(abortController.signal);
      if (!abortController.signal.aborted) setData(result);
    } catch (err) {
      if (!abortController.signal.aborted) {
        setError(err instanceof Error ? err : new Error(String(err)));
      }
    } finally {
      if (!abortController.signal.aborted) setLoading(false);
    }
  }, []);

  return { run, data, error, loading };
}

export default useLazyAsync;