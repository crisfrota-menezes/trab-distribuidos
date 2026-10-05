const API_URL = "http://127.0.0.1:8000";

// Função para buscar produtos via HTTP GET no API Gateway (MS Principal)
async function carregarProdutos() {
    const tbody = document.querySelector("#tabela-produtos tbody");
    tbody.innerHTML = "<tr><td colspan='4'>A carregar...</td></tr>";

    try {
        const response = await fetch(`${API_URL}/produtos`);
        if (!response.ok) throw new Error("Erro ao carregar produtos");

        const produtos = await response.json();
        tbody.innerHTML = "";

        produtos.forEach(p => {
            const tr = document.createElement("tr");
            tr.innerHTML = `
                <td>${p.produto_id}</td>
                <td><strong>${p.nome}</strong></td>
                <td>R$ ${p.valor.toFixed(2)}</td>
                <td>${p.quantidade > 0 ? p.quantidade + ' un' : '<span style="color:red">Esgotado</span>'}</td>
            `;
            tbody.appendChild(tr);
        });
    } catch (error) {
        tbody.innerHTML = `<tr><td colspan='4' style='color:red;'>Erro ao ligar ao API Gateway: ${error.message}</td></tr>`;
    }
}