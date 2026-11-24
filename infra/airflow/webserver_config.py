"""Login da UI do Airflow com a conta do GitHub (FAB auth manager, OAuth).

Os times da organização decidem o papel. Quem não está em nenhum time de
TIME_PARA_PAPEL entra como Public, que não tem permissão nenhuma.
Doc: https://airflow.apache.org/docs/apache-airflow-providers-fab/stable/auth-manager/webserver-authentication.html
"""

import logging
import os

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from flask_appbuilder.security.manager import AUTH_OAUTH

log = logging.getLogger(__name__)

# Time da organização (slug, como na URL do time) -> papel do Airflow.
ORGANIZACAO = "GovHub-br"
TIME_PARA_PAPEL = {
    "mcid-admin": "Admin",
    "mcid": "Op",
}

AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = True
AUTH_USER_REGISTRATION_ROLE = "Public"
AUTH_ROLES_SYNC_AT_LOGIN = True
AUTH_ROLES_MAPPING = {papel: [papel] for papel in TIME_PARA_PAPEL.values()}

OAUTH_PROVIDERS = [
    {
        "name": "github",
        "icon": "fa-github",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ["GITHUB_OAUTH_CLIENT_ID"],
            "client_secret": os.environ["GITHUB_OAUTH_CLIENT_SECRET"],
            "api_base_url": "https://api.github.com/",
            # read:org é o que permite ler os times do usuário.
            "client_kwargs": {"scope": "read:user read:org"},
            "access_token_url": "https://github.com/login/oauth/access_token",
            "authorize_url": "https://github.com/login/oauth/authorize",
            "request_token_url": None,
        },
    },
]


class GithubTeamAuthorizer(FabAirflowSecurityManagerOverride):
    def get_oauth_user_info(self, provider, resp):
        # Erro na API do GitHub vira login negado, nunca um usuário sem papel.
        remote = self.oauth_remotes[provider]
        resposta = remote.get("user")
        resposta.raise_for_status()
        usuario = resposta.json()
        resposta = remote.get("user/teams", params={"per_page": 100})
        resposta.raise_for_status()

        # O slug só é único dentro da organização: conferir as duas coisas.
        slugs = {
            time["slug"] for time in resposta.json() if time["organization"]["login"] == ORGANIZACAO
        }
        papeis = sorted({TIME_PARA_PAPEL[s] for s in slugs if s in TIME_PARA_PAPEL})
        log.info("Login GitHub: usuario=%s papeis=%s", usuario["login"], papeis)
        return {
            "username": f"github_{usuario['login']}",
            "email": usuario.get("email") or "",
            "first_name": usuario.get("name") or usuario["login"],
            "last_name": "",
            "role_keys": papeis,
        }


SECURITY_MANAGER_CLASS = GithubTeamAuthorizer
