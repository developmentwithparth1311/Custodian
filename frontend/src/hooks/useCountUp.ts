import { useEffect, useRef, useState } from "react";

export function useCountUp(targetValue: number, duration: number = 600): number {
  const [displayValue, setDisplayValue] = useState<number>(targetValue);
  const prevValueRef = useRef<number>(targetValue);
  const animFrameRef = useRef<number | null>(null);

  useEffect(() => {
    const startVal = prevValueRef.current;
    const endVal = targetValue;
    
    // If values are identical or non-numeric, sync directly
    if (startVal === endVal || isNaN(endVal)) {
      setDisplayValue(endVal);
      prevValueRef.current = endVal;
      return;
    }

    const startTime = performance.now();

    const animate = (currentTime: number) => {
      const elapsed = currentTime - startTime;
      const progress = Math.min(elapsed / duration, 1);
      
      // Quartic ease-out: 1 - (1 - t)^4
      const easeOut = 1 - Math.pow(1 - progress, 4);
      const current = Math.round(startVal + (endVal - startVal) * easeOut);

      setDisplayValue(current);

      if (progress < 1) {
        animFrameRef.current = requestAnimationFrame(animate);
      } else {
        setDisplayValue(endVal);
        prevValueRef.current = endVal;
      }
    };

    animFrameRef.current = requestAnimationFrame(animate);

    return () => {
      if (animFrameRef.current) {
        cancelAnimationFrame(animFrameRef.current);
      }
    };
  }, [targetValue, duration]);

  return displayValue;
}
