const API_URL = window.location.hostname.includes("github.dev")
    ? `https://${window.location.hostname.replace("-3000.", "-8000.")}`
    : "http://127.0.0.1:8000";

let produtosCatalogo = [], pedidoAtual = [], pedidosFinalizados = [];
let produtoSelecionado = null, quantidadeSelecionada = 1;
const conexoesSSE = {};

const STATUS_MAP = {
    "criado": { dot: "aguardando", label: "Aguardando Processamento", css: "status-aguardando" },
    "estoque_ok": { dot: "estoque-ok", label: "Estoque Confirmado", css: "status-estoque-ok" },
    "pagamento_aguardando": { dot: "aguardando", label: "Aguardando Pagamento", css: "status-aguardando" },
    "pagamento_aprovado": { dot: "aprovado", label: "Pagamento Aprovado", css: "status-aprovado" },
    "enviado": { dot: "enviado", label: "Enviado", css: "status-enviado" },
    "excluido": { dot: "error", label: "Pedido Cancelado/Recusado", css: "status-error" }
};

const $ = (id) => document.getElementById(id);

function navegarPara(telaId) {
    ["tela-catalogo", "tela-adicionar-pedido", "tela-consultar-pedidos"].forEach(id => {
        const el = $(id);
        if (el) el.classList.toggle("hidden", id !== telaId);
    });
    window.scrollTo({ top: 0, behavior: "smooth" });
}

function conectarSSE(pedidoId) {
    if (conexoesSSE[pedidoId]) return;

    const source = new EventSource(`${API_URL}/pedidos/stream/${pedidoId}`);
    conexoesSSE[pedidoId] = source;

    source.onmessage = (event) => {
        try {
            const data = JSON.parse(event.data);
            const pedido = pedidosFinalizados.find(p => p.id === pedidoId);

            if (pedido && data.status) {
                pedido.status = data.status;
                if (data.checkout_url) {
                    pedido.checkout_url = data.checkout_url;
                }
                salvarHistoricoLocalStorage();
                atualizarStatusProcesso();

                // Se estiver na tela de histórico, re-renderiza os cards
                if (!$("tela-consultar-pedidos").classList.contains("hidden")) {
                    renderizarHistoricoPedidos();
                }
            }

            // Encerra o SSE quando o pedido atinge um estado final
            if (["enviado", "excluido"].includes(data.status)) {
                source.close();
                delete conexoesSSE[pedidoId];
            }
        } catch (e) {
            console.error("Erro ao processar mensagem SSE:", e);
        }
    };

    source.onerror = () => {
        source.close();
        delete conexoesSSE[pedidoId];
    };
}

async function carregarProdutos() {
    try {
        const res = await fetch(`${API_URL}/produtos`);
        if (!res.ok) throw new Error();
        produtosCatalogo = await res.json();
    } catch {
        produtosCatalogo = [
            { produto_id: 1, nome: "Monster Tradicional", valor: 12.00, quantidade: 30 },
            { produto_id: 2, nome: "Monster Mango Loco", valor: 12.00, quantidade: 40 },
            { produto_id: 3, nome: "Monster Ultra White", valor: 12.00, quantidade: 50 },
            { produto_id: 4, nome: "Monster Pacific Punch", valor: 12.00, quantidade: 20 },
            { produto_id: 5, nome: "Monster Pipeline Punch", valor: 12.00, quantidade: 15 },
            { produto_id: 6, nome: "Monster Ultra Watermelon", valor: 12.00, quantidade: 10 },
            { produto_id: 7, nome: "Monster Rio Punch", valor: 12.00, quantidade: 0 }
        ];
    }
    renderizarCatalogo();
}

function renderizarCatalogo() {
    const grid = $("grid-produtos");
    if (!produtosCatalogo.length) {
        grid.innerHTML = `<div class="empty-state"><p>Nenhum produto disponível.</p></div>`;
        return;
    }

    grid.innerHTML = produtosCatalogo.map(p => {
        const esgotado = p.quantidade <= 0;
        return `
            <div class="product-card ${esgotado ? 'esgotado' : ''}" 
                 onclick="${esgotado ? '' : `abrirTelaAdicionar(${p.produto_id})`}">
                <div class="product-card-top">
                    <h3 class="product-name">${p.nome}</h3>
                    <span class="product-stock">${esgotado ? 'Sem estoque' : `Estoque: ${p.quantidade} un`}</span>
                </div>
                <div class="product-card-bottom">
                    <span class="product-price">R$ ${p.valor.toFixed(2)}</span>
                    <button type="button" class="btn-card-select" ${esgotado ? 'disabled' : ''}>
                        ${esgotado ? 'Esgotado' : 'Selecionar'}
                    </button>
                </div>
            </div>`;
    }).join("");
}

function abrirTelaAdicionar(produtoId) {
    const p = produtosCatalogo.find(item => item.produto_id === produtoId);
    if (!p || p.quantidade <= 0) return;

    produtoSelecionado = p;
    quantidadeSelecionada = 1;

    $("detalhe-produto-nome").textContent = p.nome;
    $("detalhe-produto-preco").textContent = `R$ ${p.valor.toFixed(2)}`;
    $("detalhe-produto-estoque").textContent = `Estoque disponível: ${p.quantidade} unidades`;

    atualizarCalculoQuantidade();
    navegarPara("tela-adicionar-pedido");
}

function atualizarCalculoQuantidade() {
    if (!produtoSelecionado) return;
    $("input-qtd-selecionada").value = quantidadeSelecionada;
    $("detalhe-subtotal-item").textContent = `R$ ${(produtoSelecionado.valor * quantidadeSelecionada).toFixed(2)}`;
    $("btn-diminuir-qtd").disabled = quantidadeSelecionada <= 1;
    $("btn-aumentar-qtd").disabled = quantidadeSelecionada >= produtoSelecionado.quantidade;
}

function adicionarItemAoPedido(p, qtd) {
    const item = pedidoAtual.find(i => i.produto_id === p.produto_id);
    if (item) {
        item.quantidade = Math.min(item.quantidade + qtd, p.quantidade);
    } else {
        pedidoAtual.push({ produto_id: p.produto_id, nome: p.nome, valor: p.valor, quantidade: qtd });
    }
    atualizarUIPedidoAtual();
}

function removerItemDoPedido(produtoId) {
    pedidoAtual = pedidoAtual.filter(i => i.produto_id !== produtoId);
    atualizarUIPedidoAtual();
}

function atualizarUIPedidoAtual() {
    const totalItens = pedidoAtual.reduce((a, b) => a + b.quantidade, 0);
    const totalValor = pedidoAtual.reduce((a, b) => a + (b.valor * b.quantidade), 0);

    $("indicador-pedido-topo")?.classList.toggle("hidden", totalItens === 0);
    if (totalItens > 0) {
        $("indicador-qtd").textContent = `${totalItens} ${totalItens === 1 ? 'item' : 'itens'}`;
        $("indicador-total").textContent = `R$ ${totalValor.toFixed(2)}`;
    }

    $("painel-pedido-andamento")?.classList.toggle("hidden", pedidoAtual.length === 0);
    if (pedidoAtual.length > 0) {
        $("badge-total-itens-andamento").textContent = `${totalItens} itens`;
        $("total-pedido-andamento").textContent = `R$ ${totalValor.toFixed(2)}`;
        $("lista-itens-andamento").innerHTML = pedidoAtual.map(i => `
            <li class="item-andamento-row">
                <div class="item-info">
                    <span class="item-qtd-badge">${i.quantidade}x</span>
                    <span class="item-nome">${i.nome}</span>
                </div>
                <div class="item-acoes">
                    <span class="item-subtotal">R$ ${(i.valor * i.quantidade).toFixed(2)}</span>
                    <button type="button" class="btn-remover-item" onclick="removerItemDoPedido(${i.produto_id})">✕</button>
                </div>
            </li>`).join("");
    }
    atualizarStatusProcesso();
}

async function finalizarPedido() {
    if (!pedidoAtual.length) return;

    const payload = {
        produtos: pedidoAtual.map(i => ({ produto_id: i.produto_id, quantidade: i.quantidade }))
    };

    try {
        const res = await fetch(`${API_URL}/pedidos`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload)
        });

        if (!res.ok) {
            const err = await res.json().catch(() => ({}));
            throw new Error(err.detail || "Erro ao registrar pedido.");
        }

        const result = await res.json();
        const novoPedido = {
            id: result.pedido_id,
            data: new Date().toLocaleString("pt-BR"),
            itens: [...pedidoAtual],
            total: pedidoAtual.reduce((a, b) => a + (b.valor * b.quantidade), 0),
            status: "criado"
        };

        pedidosFinalizados.unshift(novoPedido);
        salvarHistoricoLocalStorage();

        conectarSSE(novoPedido.id);

        pedidoAtual = [];
        produtoSelecionado = null;

        atualizarUIPedidoAtual();
        atualizarBotaoConsultarPedidos();
        navegarPara("tela-consultar-pedidos");
        renderizarHistoricoPedidos();

    } catch (error) {
        alert(`Erro: ${error.message}`);
    }
}

function atualizarStatusProcesso() {
    const totalItens = pedidoAtual.reduce((a, b) => a + b.quantidade, 0);

    if (totalItens > 0) {
        definirDotStatus("montagem", `${totalItens} item(ns) em montagem`);
        return;
    }

    if (pedidosFinalizados.length > 0) {
        const ultimo = pedidosFinalizados[0];
        const config = STATUS_MAP[ultimo.status] || { dot: "aguardando", label: ultimo.status };
        definirDotStatus(config.dot, `${config.label}`);
        return;
    }

    definirDotStatus("empty", "Não há pedidos");
}

function definirDotStatus(tipo, texto) {
    if ($("dot-indicador")) $("dot-indicador").className = `dot dot-${tipo}`;
    if ($("status-texto")) $("status-texto").textContent = texto;
}

function atualizarBotaoConsultarPedidos() {
    const count = pedidosFinalizados.length;
    $("btn-nav-pedidos").disabled = count === 0;
    $("badge-pedidos-count").textContent = count;
    $("badge-pedidos-count").classList.toggle("hidden", count === 0);
    atualizarStatusProcesso();
}

function renderizarHistoricoPedidos() {
    const container = $("container-pedidos-historico");
    if (!pedidosFinalizados.length) {
        container.innerHTML = `<div class="empty-state"><p>Nenhum pedido realizado ainda.</p></div>`;
        return;
    }

    container.innerHTML = pedidosFinalizados.map(p => {
        const config = STATUS_MAP[p.status] || { label: p.status, css: "status-aguardando" };
        const itensHtml = p.itens.map(i => `
            <tr>
                <td>${i.nome}</td>
                <td>${i.quantidade}x</td>
                <td>R$ ${i.valor.toFixed(2)}</td>
                <td class="col-valor">R$ ${(i.quantidade * i.valor).toFixed(2)}</td>
            </tr>`).join("");

        const botaoCheckout = (p.status === "pagamento_aguardando" && p.checkout_url)
            ? `<a href="${p.checkout_url}" target="_blank" class="btn btn-primary btn-card-checkout">Pagar Agora</a>`
            : "";

        return `
            <div class="pedido-card">
                <div class="pedido-card-top">
                    <div class="pedido-identificador">
                        <span class="pedido-id-tag">Pedido #${p.id}</span>
                        <span class="pedido-data">${p.data}</span>
                    </div>
                    <span class="status-badge ${config.css}">${config.label}</span>
                </div>
                <table class="pedido-itens-tabela">
                    <thead>
                        <tr><th>Produto</th><th>Qtd</th><th>Valor Unit.</th><th class="col-valor">Subtotal</th></tr>
                    </thead>
                    <tbody>${itensHtml}</tbody>
                </table>
                <div class="pedido-card-footer">
                    <div class="pedido-total-box">
                        <span class="pedido-total-label">Total do Pedido:</span>
                        <strong class="pedido-total-valor">R$ ${p.total.toFixed(2)}</strong>
                    </div>
                    ${botaoCheckout}
                </div>
            </div>`;
    }).join("");
}

function salvarHistoricoLocalStorage() {
    localStorage.setItem("monster_pedidos_finalizados", JSON.stringify(pedidosFinalizados));
}

function carregarHistoricoLocalStorage() {
    try {
        const salvos = localStorage.getItem("monster_pedidos_finalizados");
        if (salvos) {
            pedidosFinalizados = JSON.parse(salvos);
            atualizarBotaoConsultarPedidos();

            // Reconecta o SSE para pedidos que ainda estão em processamento
            pedidosFinalizados.forEach(p => {
                if (!["enviado", "excluido"].includes(p.status)) {
                    conectarSSE(p.id);
                }
            });
        }
    } catch (e) {
        console.warn("Erro ao ler localStorage:", e);
    }
}

document.addEventListener("DOMContentLoaded", () => {
    carregarHistoricoLocalStorage();
    carregarProdutos();
    atualizarStatusProcesso();
    navegarPara("tela-catalogo");

    // Eventos de Quantidade
    $("btn-diminuir-qtd")?.addEventListener("click", () => {
        if (quantidadeSelecionada > 1) { quantidadeSelecionada--; atualizarCalculoQuantidade(); }
    });

    $("btn-aumentar-qtd")?.addEventListener("click", () => {
        if (produtoSelecionado && quantidadeSelecionada < produtoSelecionado.quantidade) {
            quantidadeSelecionada++; atualizarCalculoQuantidade();
        }
    });

    // Eventos de Botões
    $("btn-acao-adicionar")?.addEventListener("click", () => {
        if (produtoSelecionado) { adicionarItemAoPedido(produtoSelecionado, quantidadeSelecionada); navegarPara("tela-catalogo"); }
    });

    $("btn-acao-finalizar")?.addEventListener("click", () => {
        if (produtoSelecionado) { adicionarItemAoPedido(produtoSelecionado, quantidadeSelecionada); finalizarPedido(); }
    });

    $("btn-finalizar-pedido-resumo")?.addEventListener("click", finalizarPedido);

    $("btn-limpar-pedido")?.addEventListener("click", () => {
        if (confirm("Limpar os itens do pedido atual?")) { pedidoAtual = []; atualizarUIPedidoAtual(); }
    });

    $("btn-nav-pedidos")?.addEventListener("click", () => { renderizarHistoricoPedidos(); navegarPara("tela-consultar-pedidos"); });
    $("btn-voltar-catalogo-1")?.addEventListener("click", () => navegarPara("tela-catalogo"));
    $("btn-voltar-catalogo-2")?.addEventListener("click", () => navegarPara("tela-catalogo"));
    $("logo-header")?.addEventListener("click", () => navegarPara("tela-catalogo"));
});

async function inscreverPromocao() {
    const emailInput = $("input-promocao-email");
    const email = emailInput?.value.trim();
    const checkboxes = document.querySelectorAll("input[name='categoria']:checked");
    const categorias = Array.from(checkboxes).map(cb => cb.value);

    if (!email) {
        exibirFeedbackPromocao("Por favor, informe seu e-mail.", "erro");
        return;
    }

    if (categorias.length === 0) {
        exibirFeedbackPromocao("Selecione pelo menos uma categoria de promoção.", "erro");
        return;
    }

    const btn = $("btn-inscrever-promocao");
    const textoOriginal = btn.innerHTML;
    btn.disabled = true;
    btn.textContent = "Inscrevendo...";

    try {
        const res = await fetch(`${API_URL}/promocoes/inscrever`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email, categorias })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || "Erro ao inscrever.");

        exibirFeedbackPromocao(`${data.mensagem || "Inscrição realizada com sucesso! Fique atento ao seu e-mail."}`, "sucesso");
    } catch (err) {
        exibirFeedbackPromocao(`Erro: ${err.message}`, "erro");
    } finally {
        btn.disabled = false;
        btn.innerHTML = textoOriginal;
    }
}

async function cancelarPromocao() {
    const emailInput = $("input-promocao-email");
    const email = emailInput?.value.trim();

    if (!email) {
        exibirFeedbackPromocao("Digite seu e-mail no campo acima para cancelar a inscrição.", "erro");
        emailInput?.focus();
        return;
    }

    const btn = $("btn-cancelar-promocao");
    const textoOriginal = btn.innerHTML;
    btn.disabled = true;
    btn.textContent = "Cancelando...";

    try {
        const res = await fetch(`${API_URL}/promocoes/cancelar`, {
            method: "DELETE",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ email })
        });

        const data = await res.json();
        if (!res.ok) throw new Error(data.detail || "Erro ao cancelar inscrição.");

        exibirFeedbackPromocao(`${data.mensagem || "Inscrição cancelada com sucesso."}`, "sucesso");
    } catch (err) {
        exibirFeedbackPromocao(`Erro: ${err.message}`, "erro");
    } finally {
        btn.disabled = false;
        btn.innerHTML = textoOriginal;
    }
}

function exibirFeedbackPromocao(mensagem, tipo) {
    const el = $("promocao-feedback");
    if (!el) return;
    el.textContent = mensagem;
    el.className = `promocao-feedback feedback-${tipo}`;
    el.classList.remove("hidden");
}