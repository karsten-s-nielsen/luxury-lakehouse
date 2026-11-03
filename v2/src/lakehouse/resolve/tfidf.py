"""TF-IDF character n-gram candidates, as in the IRIS resolver (spec §15.1; plan Task 22).

This module is the typed boundary over scikit-learn and sparse_dot_topn, which ship without type stubs: the
untyped calls stay here, and the rest of the resolver sees plain Python values.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import numpy as np
from scipy import sparse  # pyright: ignore[reportMissingTypeStubs]
from sklearn.feature_extraction.text import TfidfVectorizer  # pyright: ignore[reportMissingTypeStubs]
from sparse_dot_topn import sp_matmul_topn  # pyright: ignore[reportMissingTypeStubs, reportUnknownVariableType]

__all__ = ["char_ngrams", "top_pairs"]


def char_ngrams(text: str, n: int = 3) -> list[str]:
    """The character n-grams of `text` with its whitespace removed (IRIS `_build_ngrams`)."""
    compact = text.replace(" ", "")
    return [compact[i : i + n] for i in range(max(len(compact) - n + 1, 0))]


def top_pairs(
    names_a: Sequence[str], names_b: Sequence[str], *, top_n: int, threshold: float, ngram_size: int = 3
) -> list[tuple[int, int, float]]:
    """For each name of `names_a`, the at most `top_n` names of `names_b` with a TF-IDF cosine similarity of at least
    `threshold`, as `(index_a, index_b, similarity)`. The vectoriser is fitted on both lists, as in IRIS."""
    if not names_a or not names_b:
        return []
    if not any(char_ngrams(name, ngram_size) for name in [*names_a, *names_b]):
        return []  # no n-gram at all: scikit-learn would refuse the empty vocabulary

    def analyzer(text: str) -> list[str]:
        return char_ngrams(text, ngram_size)

    vectorizer: Any = TfidfVectorizer(analyzer=analyzer, dtype=np.float32)  # pyright: ignore[reportArgumentType]
    vectorizer.fit([*names_a, *names_b])
    matrix_a: Any = vectorizer.transform(list(names_a))
    matrix_b: Any = vectorizer.transform(list(names_b))
    similarity: Any = sp_matmul_topn(matrix_a, matrix_b.T, top_n=top_n, threshold=threshold)
    coo: Any = sparse.coo_matrix(similarity)  # pyright: ignore[reportUnknownMemberType]
    return [(int(r), int(c), float(v)) for r, c, v in zip(coo.row, coo.col, coo.data, strict=True)]
