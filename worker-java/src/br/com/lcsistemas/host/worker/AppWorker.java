package br.com.lcsistemas.host.worker;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

public class AppWorker {

    public static void main(String[] args) throws IOException {
        System.out.println("Iniciando Worker Java Local na porta 8080...");
        
        HttpServer server = HttpServer.create(new InetSocketAddress(8080), 0);
        
        // Rota de Healthcheck
        server.createContext("/", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                String response = "Worker Java Online!";
                exchange.sendResponseHeaders(200, response.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(response.getBytes());
                os.close();
            }
        });

        // Rota que simula o Webhook recebendo os uploads pelo Next.js
        server.createContext("/api/processar", new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                System.out.println("Recebido um chamado de migração!");
                
                // Set CORS headers
                exchange.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
                exchange.getResponseHeaders().add("Content-Type", "application/json");

                if ("OPTIONS".equals(exchange.getRequestMethod())) {
                    exchange.getResponseHeaders().add("Access-Control-Allow-Methods", "GET, OPTIONS, POST");
                    exchange.getResponseHeaders().add("Access-Control-Allow-Headers", "Content-Type,Authorization");
                    exchange.sendResponseHeaders(204, -1);
                    return;
                }

                String jsonResponse = "{ \"status\": \"Pendente\", \"mensagem\": \"Arquivo FDB recebido, iniciando motor de geração direta de .SQL...\" }";
                
                // Futuramente aqui vamos instanciar nosso MigracaoEngine e passar o caminho do FDB salvo.

                exchange.sendResponseHeaders(200, jsonResponse.getBytes().length);
                OutputStream os = exchange.getResponseBody();
                os.write(jsonResponse.getBytes());
                os.close();
            }
        });

        server.setExecutor(null); // cria um executor default
        server.start();
        
        System.out.println("Servidor escutando conexões na porta 8080");
        System.out.println("http://localhost:8080/");
    }
}
