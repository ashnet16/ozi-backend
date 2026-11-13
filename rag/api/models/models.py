from pydantic import BaseModel, Field


class QueryRequest(BaseModel):
    query: str = Field(
        ..., max_length=512, description="Query must be 512 characters or fewer"
    )
    fid: int | None = None
    is_comment: bool | None = None
    n_results: int = 1500
    min_score: float = 0.5
