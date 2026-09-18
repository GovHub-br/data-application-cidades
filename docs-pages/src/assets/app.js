/*
 * app.js — comportamento opcional do site.
 *
 * Progressive enhancement: nada aqui e necessario para ler a pagina. Sem
 * JavaScript, as tabelas continuam completas e o conteudo inteiro permanece
 * acessivel e imprimivel.
 */

(function () {
  "use strict";

  /** Filtra linhas de uma tabela pelo texto digitado. */
  function ligarFiltro(campo) {
    var alvo = document.getElementById(campo.dataset.filtroAlvo);
    if (!alvo) return;

    var linhas = Array.prototype.slice.call(alvo.querySelectorAll("tbody tr"));
    var contador = document.getElementById(campo.dataset.filtroContador);

    campo.addEventListener("input", function () {
      var termo = campo.value.trim().toLowerCase();
      var visiveis = 0;

      linhas.forEach(function (linha) {
        var casa = !termo || linha.textContent.toLowerCase().indexOf(termo) !== -1;
        linha.hidden = !casa;
        if (casa) visiveis++;
      });

      if (contador) {
        contador.textContent =
          visiveis === linhas.length
            ? linhas.length + " registros"
            : visiveis + " de " + linhas.length + " registros";
      }
    });
  }

  document.addEventListener("DOMContentLoaded", function () {
    document.querySelectorAll("[data-filtro-alvo]").forEach(ligarFiltro);
  });
})();
