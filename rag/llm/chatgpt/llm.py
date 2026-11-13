from typing import List

from llm.base import BaseLLM
from openai import AsyncOpenAI


class ChatGPTLLM(BaseLLM):
    def __init__(self):
        self.client = None

    def connect(self, connection_info):
        self.client = AsyncOpenAI(
            api_key=connection_info.api_key, base_url=connection_info.base_url
        )

    async def ask(self, prompt: str) -> str:
        return await self.generate(prompt)

    async def summarize(self, user_query: str, documents: List[str]) -> str:
        context = "\n\n".join(documents)
        prompt = (
            f"Given the following documents:\n\n{context}\n\n"
            f"Answer the following question in a clear and concise way:\n{user_query}"
        )
        return await self.generate(
            prompt,
            system_prompt="You are a helpful assistant summarizing search results.",
        )

    async def generate(
        self, prompt: str, system_prompt: str = "You are a helpful assistant."
    ) -> str:
        base_messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": prompt},
        ]

        try:
            response = await self.client.chat.completions.create(
                model="gpt-4-turbo",
                messages=base_messages,
                temperature=0.3,
                max_tokens=500,
            )
        except Exception as e:
            if "model" in str(e).lower() or "not found" in str(e).lower():
                print("Falling back to gpt-3.5-turbo...")
                response = await self.client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=base_messages,
                    temperature=0.3,
                    max_tokens=500,
                )
            else:
                raise

        return response.choices[0].message.content
