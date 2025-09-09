from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import pandas as pd
from typing import List, Dict, Union

class SentimentModel:
    def __init__(self):
        self.model_name = "cardiffnlp/twitter-roberta-base-sentiment"
        self.tokenizer = AutoTokenizer.from_pretrained(self.model_name)
        self.model = AutoModelForSequenceClassification.from_pretrained(self.model_name)
        self.model.eval()
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.model.to(self.device)
        self.batch_size = 32

    def predict(self, inputs: List[Dict[str, str]]) -> List[Dict[str, Union[str, float]]]:
        for i in range(0, len(inputs), self.batch_size):
            batch_dicts = inputs[i:i + self.batch_size]
            batch_texts = [d['body'] for d in batch_dicts]
            batch_tokenized = self.tokenizer(batch_texts, return_tensors="pt", padding=True, truncation=True)
            batch_tokenized = {k: v.to(self.device) for k, v in batch_tokenized.items()}

            with torch.no_grad():
                outputs = self.model(**batch_tokenized)
                scores = torch.nn.functional.softmax(outputs.logits, dim=-1)

            for d, score_vector in zip(batch_dicts, scores):
                d['negative_score'] = score_vector[0].item()
                d['neutral_score'] = score_vector[1].item()
                d['positive_score'] = score_vector[2].item()
                d['pred_label'] = ["negative", "neutral", "positive"][torch.argmax(score_vector).item()]

        return inputs
