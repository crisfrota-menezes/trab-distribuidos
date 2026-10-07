const API_URL = window.location.hostname.includes("github.dev")
    ? `https://${window.location.hostname.replace("-3000.", "-8000.")}`
    : "http://127.0.0.1:8000";

let produtosCatalogo = [];
let pedidoAtual = [];
let pedidosFinalizados = [];
let produtoSelecionado = null;
let quantidadeSelecionada = 1;

const telaCatalogo = document.getElementById("tela-catalogo");
const telaAdicionar = document.getElementById("tela-adicionar-pedido");
const telaConsultar = document.getElementById("tela-consultar-pedidos");

const btnNavPedidos = document.getElementById("btn-nav-pedidos");
const badgePedidosCount = document.getElementById("badge-pedidos-count");
const logoHeader = document.getElementById("logo-header");
const indicadorPedidoTopo = document.getElementById("indicador-pedido-topo");
const indicadorQtd = document.getElementById("indicador-qtd");
const indicadorTotal = document.getElementById("indicador-total");
const dotIndicador = document.getElementById("dot-indicador");
const statusTexto = document.getElementById("status-texto");
const statusPedidosContainer = document.getElementById("status-pedidos");

const gridProdutos = document.getElementById("grid-produtos");
const painelPedidoAndamento = document.getElementById("painel-pedido-andamento");
const listaItensAndamento = document.getElementById("lista-itens-andamento");
const badgeTotalItensAndamento = document.getElementById("badge-total-itens-andamento");
const totalPedidoAndamento = document.getElementById("total-pedido-andamento");
const btnFinalizarPedidoResumo = document.getElementById("btn-finalizar-pedido-resumo");
const btnLimparPedido = document.getElementById("btn-limpar-pedido");

const btnVoltarCatalogo1 = document.getElementById("btn-voltar-catalogo-1");
const detalheProdutoNome = document.getElementById("detalhe-produto-nome");
const detalheProdutoPreco = document.getElementById("detalhe-produto-preco");
const detalheProdutoEstoque = document.getElementById("detalhe-produto-estoque");
const inputQtdSelecionada = document.getElementById("input-qtd-selecionada");
const btnDiminuirQtd = document.getElementById("btn-diminuir-qtd");
const btnAumentarQtd = document.getElementById("btn-aumentar-qtd");
const detalheSubtotalItem = document.getElementById("detalhe-subtotal-item");
const btnAcaoAdicionar = document.getElementById("btn-acao-adicionar");
const btnAcaoFinalizar = document.getElementById("btn-acao-finalizar");

const btnVoltarCatalogo2 = document.getElementById("btn-voltar-catalogo-2");
const containerPedidosHistorico = document.getElementById("container-pedidos-historico");

function navegarPara(telaId) {
    telaCatalogo.classList.add("hidden");
    telaAdicionar.classList.add("hidden");
    telaConsultar.classList.add("hidden");

    const telaDestino = document.getElementById(telaId);
    if (telaDestino) {
        telaDestino.classList.remove("hidden");
        window.scrollTo({ top: 0, behavior: "smooth" });
    }
}

async function carregarProdutos() {
    try {
        const response = await fetch(`${API_URL}/produtos`);
        if (!response.ok) throw new Error("Falha ao comunicar com o Gateway/Serviço");

        produtosCatalogo = await response.json();
        renderizarCatalogo(produtosCatalogo);
    } catch (error) {
        console.warn("Aviso ao buscar API (usando catálogo padrão):", error);
        produtosCatalogo = [
            { produto_id: 1, nome: "Monster Tradicional", valor: 12.00, quantidade: 30 },
            { produto_id: 2, nome: "Monster Mango Loco", valor: 12.00, quantidade: 40 },
            { produto_id: 3, nome: "Monster Ultra White", valor: 12.00, quantidade: 50 },
            { produto_id: 4, nome: "Monster Pacific Punch", valor: 12.00, quantidade: 20 },
            { produto_id: 5, nome: "Monster Pipeline Punch", valor: 12.00, quantidade: 15 },
            { produto_id: 6, nome: "Monster Ultra Watermelon", valor: 12.00, quantidade: 10 },
            { produto_id: 7, nome: "Monster Rio Punch", valor: 12.00, quantidade: 0 }
        ];
        renderizarCatalogo(produtosCatalogo);
    }
}

function renderizarCatalogo(produtos) {
    if (!produtos || produtos.length === 0) {
        gridProdutos.innerHTML = `
            <div class="empty-state">
                <p>Nenhum produto disponível no momento.</p>
            </div>
        `;
        return;
    }

    gridProdutos.innerHTML = produtos.map(p => {
        const esgotado = p.quantidade <= 0;
        return `
            <div class="product-card ${esgotado ? 'esgotado' : ''}" 
                 onclick="${esgotado ? '' : `abrirTelaAdicionar(${p.produto_id})`}">
                
                <div class="product-card-top">
                    <h3 class="product-name">${p.nome}</h3>
                    <span class="product-stock">
                        ${esgotado ? 'Sem estoque' : `Estoque: ${p.quantidade} un`}
                    </span>
                </div>

                <div class="product-card-bottom">
                    <span class="product-price">R$ ${p.valor.toFixed(2)}</span>
                    <button type="button" class="btn-card-select" ${esgotado ? 'disabled' : ''}>
                        ${esgotado ? 'Esgotado' : 'Selecionar'}
                    </button>
                </div>
            </div>
        `;
    }).join("");
}

function abrirTelaAdicionar(produtoId) {
    const produto = produtosCatalogo.find(p => p.produto_id === produtoId);
    if (!produto || produto.quantidade <= 0) return;

    produtoSelecionado = produto;
    quantidadeSelecionada = 1;

    detalheProdutoNome.textContent = produto.nome;
    detalheProdutoPreco.textContent = `R$ ${produto.valor.toFixed(2)}`;
    detalheProdutoEstoque.textContent = `Estoque disponível: ${produto.quantidade} unidades`;

    inputQtdSelecionada.value = quantidadeSelecionada;
    inputQtdSelecionada.max = produto.quantidade;

    atualizarCalculoQuantidade();
    navegarPara("tela-adicionar-pedido");
}

function atualizarCalculoQuantidade() {
    if (!produtoSelecionado) return;

    inputQtdSelecionada.value = quantidadeSelecionada;
    const subtotal = produtoSelecionado.valor * quantidadeSelecionada;
    detalheSubtotalItem.textContent = `R$ ${subtotal.toFixed(2)}`;

    btnDiminuirQtd.disabled = quantidadeSelecionada <= 1;
    btnAumentarQtd.disabled = quantidadeSelecionada >= produtoSelecionado.quantidade;
}

btnDiminuirQtd.addEventListener("click", () => {
    if (quantidadeSelecionada > 1) {
        quantidadeSelecionada--;
        atualizarCalculoQuantidade();
    }
});

btnAumentarQtd.addEventListener("click", () => {
    if (produtoSelecionado && quantidadeSelecionada < produtoSelecionado.quantidade) {
        quantidadeSelecionada++;
        atualizarCalculoQuantidade();
    }
});

btnAcaoAdicionar.addEventListener("click", () => {
    if (!produtoSelecionado) return;
    adicionarItemAoPedido(produtoSelecionado, quantidadeSelecionada);
    navegarPara("tela-catalogo");
});

btnAcaoFinalizar.addEventListener("click", () => {
    if (!produtoSelecionado) return;
    adicionarItemAoPedido(produtoSelecionado, quantidadeSelecionada);
    finalizarPedido();
});

function adicionarItemAoPedido(produto, qtd) {
    const itemExistente = pedidoAtual.find(item => item.produto_id === produto.produto_id);

    if (itemExistente) {
        const novaQtd = itemExistente.quantidade + qtd;
        itemExistente.quantidade = novaQtd > produto.quantidade ? produto.quantidade : novaQtd;
    } else {
        pedidoAtual.push({
            produto_id: produto.produto_id,
            nome: produto.nome,
            valor: produto.valor,
            quantidade: qtd
        });
    }

    atualizarUIPedidoAtual();
}

function removerItemDoPedido(produtoId) {
    pedidoAtual = pedidoAtual.filter(item => item.produto_id !== produtoId);
    atualizarUIPedidoAtual();
}

function limparPedidoAtual() {
    pedidoAtual = [];
    atualizarUIPedidoAtual();
}

function atualizarUIPedidoAtual() {
    const totalItens = pedidoAtual.reduce((acc, item) => acc + item.quantidade, 0);
    const valorTotal = pedidoAtual.reduce((acc, item) => acc + (item.valor * item.quantidade), 0);

    if (totalItens > 0) {
        indicadorPedidoTopo.classList.remove("hidden");
        indicadorQtd.textContent = `${totalItens} ${totalItens === 1 ? 'item' : 'itens'}`;
        indicadorTotal.textContent = `R$ ${valorTotal.toFixed(2)}`;
    } else {
        indicadorPedidoTopo.classList.add("hidden");
    }

    if (pedidoAtual.length > 0) {
        painelPedidoAndamento.classList.remove("hidden");
        badgeTotalItensAndamento.textContent = `${totalItens} ${totalItens === 1 ? 'item' : 'itens'}`;
        totalPedidoAndamento.textContent = `R$ ${valorTotal.toFixed(2)}`;

        listaItensAndamento.innerHTML = pedidoAtual.map(item => {
            const subtotal = item.valor * item.quantidade;
            return `
                <li class="item-andamento-row">
                    <div class="item-info">
                        <span class="item-qtd-badge">${item.quantidade}x</span>
                        <span class="item-nome">${item.nome}</span>
                    </div>
                    <div class="item-acoes">
                        <span class="item-subtotal">R$ ${subtotal.toFixed(2)}</span>
                        <button type="button" class="btn-remover-item" 
                                onclick="removerItemDoPedido(${item.produto_id})">
                            ✕
                        </button>
                    </div>
                </li>
            `;
        }).join("");
    } else {
        painelPedidoAndamento.classList.add("hidden");
    }

    atualizarStatusProcesso();
}

btnFinalizarPedidoResumo.addEventListener("click", () => {
    finalizarPedido();
});

btnLimparPedido.addEventListener("click", () => {
    if (confirm("Tem certeza que deseja limpar os itens do pedido atual?")) {
        limparPedidoAtual();
    }
});

function finalizarPedido() {
    if (pedidoAtual.length === 0) return;

    const novoId = pedidosFinalizados.length + 1;
    const valorTotal = pedidoAtual.reduce((acc, item) => acc + (item.valor * item.quantidade), 0);
    const dataHora = new Date().toLocaleString("pt-BR", {
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit"
    });

    const pedidoSalvo = {
        id: novoId,
        data: dataHora,
        itens: [...pedidoAtual],
        total: valorTotal,
        status: "Aguardando Pagamento"
    };

    pedidosFinalizados.unshift(pedidoSalvo);
    salvarHistoricoLocalStorage();

    pedidoAtual = [];
    produtoSelecionado = null;
    atualizarUIPedidoAtual();

    atualizarBotaoConsultarPedidos();
    navegarPara("tela-catalogo");
}

function atualizarBotaoConsultarPedidos() {
    const qtdPedidos = pedidosFinalizados.length;

    if (qtdPedidos > 0) {
        btnNavPedidos.disabled = false;
        btnNavPedidos.removeAttribute("title");
        badgePedidosCount.textContent = qtdPedidos;
        badgePedidosCount.classList.remove("hidden");
    } else {
        btnNavPedidos.disabled = true;
        btnNavPedidos.setAttribute("title", "Nenhum pedido realizado ainda");
        badgePedidosCount.classList.add("hidden");
    }

    atualizarStatusProcesso();
}

function atualizarStatusProcesso() {
    const totalItensAtual = pedidoAtual.reduce((acc, item) => acc + item.quantidade, 0);

    if (totalItensAtual > 0) {
        definirDotStatus("montagem", `${totalItensAtual} ${totalItensAtual === 1 ? 'item' : 'itens'} no pedido em montagem`);
        return;
    }

    if (pedidosFinalizados.length > 0) {
        const ultimoPedido = pedidosFinalizados[0];
        const status = ultimoPedido.status || "Aguardando Pagamento";

        switch (status) {
            case "Aguardando Pagamento":
                definirDotStatus("aguardando", `Pedido #${ultimoPedido.id}: Aguardando Pagamento`);
                break;
            case "Estoque Confirmado":
            case "estoque_ok":
                definirDotStatus("estoque-ok", `Pedido #${ultimoPedido.id}: Estoque Confirmado`);
                break;
            case "Pagamento Aprovado":
            case "pagamento_aprovado":
                definirDotStatus("aprovado", `Pedido #${ultimoPedido.id}: Pagamento Aprovado`);
                break;
            case "Enviado":
            case "pedido.enviado":
                definirDotStatus("enviado", `Pedido #${ultimoPedido.id}: Enviado`);
                break;
            case "Recusado":
            case "pagamento_recusado":
            case "Cancelado":
                definirDotStatus("error", `Pedido #${ultimoPedido.id}: Recusado`);
                break;
            default:
                definirDotStatus("aguardando", `Pedido #${ultimoPedido.id}: ${status}`);
        }
        return;
    }

    definirDotStatus("empty", "Não há pedidos");
}

function definirDotStatus(tipo, texto) {
    if (!dotIndicador || !statusTexto) return;
    dotIndicador.className = `dot dot-${tipo}`;
    statusTexto.textContent = texto;
}

function abrirTelaConsultar() {
    if (pedidosFinalizados.length === 0) return;

    renderizarHistoricoPedidos();
    navegarPara("tela-consultar-pedidos");
}

function renderizarHistoricoPedidos() {
    if (pedidosFinalizados.length === 0) {
        containerPedidosHistorico.innerHTML = `
            <div class="empty-state">
                <p>Nenhum pedido realizado ainda.</p>
            </div>
        `;
        return;
    }

    containerPedidosHistorico.innerHTML = pedidosFinalizados.map(pedido => {
        const itensHtml = pedido.itens.map(item => `
            <tr>
                <td>${item.nome}</td>
                <td>${item.quantidade}x</td>
                <td>R$ ${item.valor.toFixed(2)}</td>
                <td class="col-valor">R$ ${(item.quantidade * item.valor).toFixed(2)}</td>
            </tr>
        `).join("");

        return `
            <div class="pedido-card">
                <div class="pedido-card-top">
                    <div class="pedido-identificador">
                        <span class="pedido-id-tag">Pedido #${pedido.id}</span>
                        <span class="pedido-data">${pedido.data}</span>
                    </div>
                    <span class="status-badge status-aguardando">
                        🟡 ${pedido.status}
                    </span>
                </div>

                <table class="pedido-itens-tabela">
                    <thead>
                        <tr>
                            <th>Produto</th>
                            <th>Qtd</th>
                            <th>Valor Unit.</th>
                            <th class="col-valor">Subtotal</th>
                        </tr>
                    </thead>
                    <tbody>
                        ${itensHtml}
                    </tbody>
                </table>

                <div class="pedido-card-footer">
                    <span class="pedido-total-label">Total do Pedido:</span>
                    <strong class="pedido-total-valor">R$ ${pedido.total.toFixed(2)}</strong>
                </div>
            </div>
        `;
    }).join("");
}

function salvarHistoricoLocalStorage() {
    try {
        localStorage.setItem("monster_pedidos_finalizados", JSON.stringify(pedidosFinalizados));
    } catch (e) {
        console.warn("Não foi possível salvar no localStorage:", e);
    }
}

function carregarHistoricoLocalStorage() {
    try {
        const salvos = localStorage.getItem("monster_pedidos_finalizados");
        if (salvos) {
            pedidosFinalizados = JSON.parse(salvos);
            atualizarBotaoConsultarPedidos();
        }
    } catch (e) {
        console.warn("Não foi possível ler do localStorage:", e);
    }
}

btnNavPedidos.addEventListener("click", () => {
    abrirTelaConsultar();
});

btnVoltarCatalogo1.addEventListener("click", () => {
    produtoSelecionado = null;
    navegarPara("tela-catalogo");
});

btnVoltarCatalogo2.addEventListener("click", () => {
    navegarPara("tela-catalogo");
});

logoHeader.addEventListener("click", () => {
    navegarPara("tela-catalogo");
});

document.addEventListener("DOMContentLoaded", () => {
    carregarHistoricoLocalStorage();
    carregarProdutos();
    atualizarStatusProcesso();
    navegarPara("tela-catalogo");
});