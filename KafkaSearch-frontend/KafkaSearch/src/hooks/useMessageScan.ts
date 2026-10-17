import { useCallback, useEffect, useRef, useState } from "react";
import type { KafkaMessage, ScanRequest } from "../types/kafka";
import { scanMessages } from "../api/kafkaClient";

export function useMessageScan() {
  const [messages, setMessages] = useState<KafkaMessage[]>([]);
  const [status, setStatus] = useState<"idle" | "running" | "done" | "error">("idle");
  const [error, setError] = useState<string | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const start = useCallback(async (request: ScanRequest) => {
    abortRef.current?.abort();
    const controller = new AbortController();
    abortRef.current = controller;

    setMessages([]);
    setError(null);
    setStatus("running");

    try {
      await scanMessages(
        request,
        (m) => setMessages((prev) => [...prev, m]),
        controller.signal
      );
      if (!controller.signal.aborted) setStatus("done");
    } catch (e) {
      if (controller.signal.aborted) return;  // aborts aren't errors
      setError(e instanceof Error ? e.message : String(e));
      setStatus("error");
    }
  }, []);

  const cancel = useCallback(() => {
    abortRef.current?.abort();
    setStatus("done");
  }, []);

  useEffect(() => () => abortRef.current?.abort(), []);

  return { messages, status, error, start, cancel };
}