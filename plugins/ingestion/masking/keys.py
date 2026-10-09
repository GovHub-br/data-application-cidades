"""Chaves do mascaramento: o segredo do HMAC, o tamanho do token e a redação."""

import hashlib
import hmac
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class MaskingKeys:
    """Como mascarar um valor. O mesmo segredo e tamanho dão o mesmo token.

    `token` (CPF, NIS): HMAC-SHA256 determinístico, truncado: irreversível, mas
    preserva join e contagem de distintos entre bases. `redact` (nome, endereço,
    CEP, nascimento): troca o valor pela redação. Vazio passa como está.
    """

    secret: bytes
    token_len: int = 16
    redaction: str = "***"

    @classmethod
    def from_env(
        cls,
        secret_env: str = "MASKING_HMAC_SECRET",
        token_len_env: str = "MASKING_TOKEN_LEN",
        redaction_env: str = "MASKING_REDACTION",
    ) -> "MaskingKeys":
        secret = os.environ.get(secret_env, "")
        if not secret:
            raise ValueError(f"segredo do mascaramento ausente: defina {secret_env}")
        return cls(
            secret=secret.encode("utf-8"),
            token_len=int(os.environ.get(token_len_env, 16)),
            redaction=os.environ.get(redaction_env, "***"),
        )

    def token(self, valor: str) -> str:
        if valor is None or valor.strip() == "":
            return valor
        dig = hmac.new(self.secret, valor.strip().encode("utf-8"), hashlib.sha256)
        return dig.hexdigest()[: self.token_len]

    def redact(self, valor: str) -> str:
        if valor is None or valor.strip() == "":
            return valor
        return self.redaction
