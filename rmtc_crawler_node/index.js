// Imports comuns
import { setTimeout } from "node:timers/promises";
import "dotenv/config";

// Puppeter
import puppeteer from "puppeteer";

// PostgreSQL
import pg from "pg";
import { timeout } from "puppeteer";

// Conexão com o pgSQL
const pgClient = new pg.Client({
    user: process.env.DB_USER,
    host: process.env.DB_HOST,
    database: process.env.DB_NAME,
    password: process.env.DB_PASS,
    port: 5432,
});
await pgClient.connect();

// Constantes de timeout
const TIMEOUT = 1000;

// Baixa os dados dos ônibus, retorna dois campos, o timestamp da requisição e uma flag se a operação foi bem sucedida
async function baixaDadosOnibus() {
    // Inicia o browser e abre uma página vazia
    const browser = await puppeteer.launch({ 
        headless: true,
        args: ["--no-sandbox", "--disable-setuid-sandbox"] // DOCKER
    });
    const page = await browser.newPage();

    // Timestamp
    const currentDate = new Date();
    const timestamp = currentDate.toISOString();

    // Flag de sucesso
    let execucaoFuncionou = true;

    // Navega para a url
    const url = "https://rmtcgoiania.com.br/index.php/olho-no-onibus?enviar=Acessar";
    await page.goto(url, {
        waitUntil: "networkidle2",
    });

    // Handle error
    if (page.url() != url) {
        console.error("Erro ao acessar a página");
        execucaoFuncionou = false;
        return [timestamp, execucaoFuncionou];
    }

    // Espera um pouco
    await setTimeout(TIMEOUT);

    // Pega os dados dos ônibus
    const fetchDadosOnibus = await page.evaluate(async () => {
        const response = await fetch("https://rmtcgoiania.com.br/index.php?option=com_rmtclinhas&view=cconaweb&format=json&linha=000", {
            headers: {
                accept: "application/json",
                "accept-language": "pt-BR,pt;q=0.9",
                priority: "u=1, i",
                "sec-ch-ua": '"Chromium";v="131", "Not_A Brand";v="24"',
                "sec-ch-ua-mobile": "?0",
                "sec-ch-ua-platform": '"Windows"',
                "sec-fetch-dest": "empty",
                "sec-fetch-mode": "cors",
                "sec-fetch-site": "same-origin",
                "x-request": "JSON",
                "x-requested-with": "XMLHttpRequest",
            },
            method: "GET",
        });
        return await response.json(); // Parse and return the JSON data
    });

    // Extraí campos
    if (fetchDadosOnibus?.status != "sucesso") {
        execucaoFuncionou = false;
    } else {
        const { onibus } = fetchDadosOnibus;

        // Para cada ônibus, insere no banco de dados
        for (const o of onibus) {
            const { Numero, Latitude, Longitude, Acessivel, Situacao, Linha, Destino } = o;
            const { LinhaNumero } = Linha;
            const { DestinoCurto } = Destino;

            let query = `
                INSERT INTO rmtc_linha_info (
                	DiaHorario, Numero, Latitude, Longitude, Acessivel, Situacao, LinhaNumero, DestinoCurto
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8
                )
                ON CONFLICT (DiaHorario, Numero) DO NOTHING
            `;

            const queryConfig = {
                text: query,
                values: [timestamp, Numero, Latitude, Longitude, Acessivel, Situacao, LinhaNumero, DestinoCurto],
                timeout: 1000,
            };

            try {
                await pgClient.query(queryConfig);
                console.log("Inserido com sucesso: ", Numero);
            } catch (error) {
                console.error("Erro ao inserir no banco de dados: ", error);
            }
        }
    }

    // Fecha o browser
    await browser.close();

    return [timestamp, true];
}

async function insereStatusOperacao(timestamp, execucaoFuncionou) {
    let query = `
        INSERT INTO rmtc_status_operacao (
            DiaHorario, ExecucaoFuncionou
        ) VALUES (
            $1, $2
        )
    `;

    const queryConfig = {
        text: query,
        values: [timestamp, execucaoFuncionou],
        timeout: 1000,
    };

    try {
        await pgClient.query(queryConfig);
        console.log("Inserido status da operacao: ", execucaoFuncionou);
    } catch (error) {
        console.error("Erro ao inserir no banco de dados: ", error);
    }
}

// Função principal
(async () => {
    // Inicia o timer
    console.time("TotalExecution");

    // Baixa os dados dos ônibus
    const [timestamp, execucaoFuncionou] = await baixaDadosOnibus();

    // Insere no banco de dados o status da operação
    await insereStatusOperacao(timestamp, execucaoFuncionou);

    // Fecha a conexão com o banco de dados
    await pgClient.end();

    // Fecha o timer
    console.timeEnd("TotalExecution");

    // Encerra o programa
    process.exit(0);
})();
