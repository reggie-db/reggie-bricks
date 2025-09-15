import base64
from dataclasses import dataclass, field
from typing import Optional, Dict, List, Tuple
from datetime import datetime, timedelta

import cv2
import torch
from ultralytics.engine.model import Model


@dataclass
class Detection:
    label: str
    confidence: float
    x1: int
    y1: int
    x2: int
    y2: int
    create_ts: datetime = field(default_factory=datetime.utcnow)

    def drawRectangle(self, frame: cv2.typing.MatLike):
        cv2.rectangle(frame, (self.x1, self.y1), (self.x2, self.y2),
                      (0, 255, 0), 2)

    def drawLabel(self, frame: cv2.typing.MatLike, ):
        cv2.putText(frame, f"{self.label} {self.confidence:.2f}", (self.x1, self.y1 - 5),
                    cv2.FONT_HERSHEY_SIMPLEX, 0.7, (0, 255, 0), 2)

    def crop(self, frame: cv2.typing.MatLike) -> Optional[bytes]:
        """
        Crop this detection from the given frame and return PNG-encoded bytes.
        Returns None if encoding fails.
        """
        h, w = frame.shape[:2]

        # clamp to image bounds
        x1 = max(0, min(self.x1, w - 1))
        x2 = max(0, min(self.x2, w - 1))
        y1 = max(0, min(self.y1, h - 1))
        y2 = max(0, min(self.y2, h - 1))
        if x2 <= x1 or y2 <= y1:
            return None

        cropped = frame[y1:y2, x1:x2]

        # encode as PNG and convert to base64 data URL
        success, buf = cv2.imencode(".png", cropped)
        if not success:
            return None
        return buf

    def crop_data_uri(self, frame: cv2.typing.MatLike) -> Optional[str]:
        """
        Crop this detection and return as base64 data URL.
        """
        buf = self.crop(frame)
        if buf is None:
            return None
        return f"data:image/png;base64,{base64.b64encode(buf).decode('utf-8')}"

    @classmethod
    def detect(cls, model: Model, frame: cv2.typing.MatLike, confidence: float = None) -> List["Detection"]:
        device = "mps" if torch.backends.mps.is_available() else ("cuda" if torch.cuda.is_available() else "cpu")
        names = model.names
        res = model(frame, verbose=False, device=device)[0]
        detections: List[Detection] = []
        for box in res.boxes:
            box_confidence = float(box.conf[0])
            if confidence is not None and confidence > box_confidence:
                continue
            label = names[int(box.cls[0])]
            x1, y1, x2, y2 = map(int, box.xyxy[0])
            detection = cls(
                label=label,
                confidence=box_confidence,
                x1=x1,
                y1=y1,
                x2=x2,
                y2=y2,
            )
            detections.append(detection)
        return detections


class DetectionTracker:
    def __init__(self, ttl: timedelta):
        self.store: Dict[str, Detection] = {}
        self.ttl = ttl

    def put(self, detection: Detection) -> Tuple[bool, List[str]]:
        added = detection.label not in self.store
        self.store[detection.label] = detection
        removed_labels: List[str] = []
        for store_label, store_detection in self.store.items():
            if detection.label == store_label:
                continue
            if datetime.utcnow() - store_detection.create_ts > self.ttl:
                removed_labels.append(store_label)
        for remove_label in removed_labels:
            self.store.pop(remove_label)
        return added, removed_labels
