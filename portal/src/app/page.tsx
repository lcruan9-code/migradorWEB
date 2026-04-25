"use client";

import { useState, useRef, useEffect, useCallback } from "react";

type JobStatus = "idle" | "PENDENTE" | "PROCESSANDO" | "CONCLUIDO" | "ERRO";

const SISTEMAS = [
  { value: "syspdv", label: "Syspdv (.FDB)" },
  { value: "gdoor",  label: "GDoor (.FDB)" },
  { value: "host",   label: "Host (.FDB)" },
  { value: "clipp",  label: "Clipp (.FBK / .FDB)" },
];

type Estado = { id: number; uf: string; nome: string; iduf: string };

const ESTADOS: Estado[] = [
  { id: 1,  uf: "AM", nome: "AMAZONAS", iduf: "13" },
  { id: 2,  uf: "AP", nome: "AMAPA", iduf: "16" },
  { id: 3,  uf: "AC", nome: "ACRE", iduf: "12" },
  { id: 4,  uf: "AL", nome: "ALAGOAS", iduf: "27" },
  { id: 5,  uf: "BA", nome: "BAHIA", iduf: "29" },
  { id: 6,  uf: "CE", nome: "CEARA", iduf: "23" },
  { id: 7,  uf: "DF", nome: "DISTRITO FEDERAL", iduf: "53" },
  { id: 8,  uf: "ES", nome: "ESPIRITO SANTO", iduf: "32" },
  { id: 9,  uf: "GO", nome: "GOIAS", iduf: "52" },
  { id: 10, uf: "MA", nome: "MARANHAO", iduf: "21" },
  { id: 11, uf: "MG", nome: "MINAS GERAIS", iduf: "31" },
  { id: 12, uf: "MS", nome: "MATO GROSSO DO SUL", iduf: "50" },
  { id: 13, uf: "MT", nome: "MATO GROSSO", iduf: "51" },
  { id: 14, uf: "PA", nome: "PARA", iduf: "15" },
  { id: 15, uf: "PB", nome: "PARAIBA", iduf: "25" },
  { id: 16, uf: "PE", nome: "PERNAMBUCO", iduf: "26" },
  { id: 17, uf: "PR", nome: "PARANA", iduf: "41" },
  { id: 18, uf: "PI", nome: "PIAUI", iduf: "22" },
  { id: 19, uf: "RJ", nome: "RIO DE JANEIRO", iduf: "33" },
  { id: 20, uf: "RN", nome: "RIO GRANDE DO NORTE", iduf: "24" },
  { id: 21, uf: "RS", nome: "RIO GRANDE DO SUL", iduf: "43" },
  { id: 22, uf: "RO", nome: "RONDONIA", iduf: "11" },
  { id: 23, uf: "RR", nome: "RORAIMA", iduf: "14" },
  { id: 24, uf: "SP", nome: "SAO PAULO", iduf: "35" },
  { id: 25, uf: "SE", nome: "SERGIPE", iduf: "28" },
  { id: 26, uf: "SC", nome: "SANTA CATARINA", iduf: "42" },
  { id: 27, uf: "TO", nome: "TOCANTINS", iduf: "17" },
  { id: 28, uf: "EX", nome: "EXTERIOR", iduf: "99" },
];
type Cidade = { id: number; nome: string };

// URL pública do worker (embedded no bundle pelo Next.js no build do Vercel).
// Chamadas pesadas (upload, status, download) vão direto do browser para o Render,
// evitando o limite de 10s e 4.5MB das serverless functions do Vercel.
const WORKER_DIRECT =
  (process.env.NEXT_PUBLIC_WORKER_URL ?? "http://localhost:8080").replace(/\/$/, "");

export default function Home() {
  const [sistema, setSistema]   = useState("syspdv");
  const [file, setFile]         = useState<File | null>(null);
  const [dragging, setDragging] = useState(false);

  // Novos campos
  const [estados, setEstados]   = useState<Estado[]>(ESTADOS);
  const [cidades, setCidades]   = useState<Cidade[]>([]);
  const [uf, setUf]             = useState("");
  const [cidade, setCidade]     = useState("");
  const [cidadeId, setCidadeId] = useState("");
  const [regime, setRegime]     = useState("SIMPLES");
  const [workerOnline, setWorkerOnline] = useState(false);
  const [checkingWorker, setCheckingWorker] = useState(true);

  const [jobId, setJobId]         = useState<string | null>(null);
  const [status, setStatus]       = useState<JobStatus>("idle");
  const [progresso, setProgresso] = useState(0);
  const [total, setTotal]         = useState(15);
  const [logs, setLogs]           = useState<string[]>([]);
  const [downloadUrl, setDownloadUrl] = useState<string | null>(null);

  const logsEndRef  = useRef<HTMLDivElement>(null);
  const pollingRef  = useRef<ReturnType<typeof setInterval> | null>(null);
  const inputRef    = useRef<HTMLInputElement>(null);

  // Carrega estados (mantém fallback local para não deixar o formulário vazio)
  useEffect(() => {
    const checkWorker = async () => {
      try {
        const r = await fetch(`/api/estados`);
        if (r.ok) {
          const data = await r.json();
          if (data && data.length > 0) setEstados(data);
          setWorkerOnline(true);
        } else {
          setWorkerOnline(false);
          setEstados(ESTADOS);
        }
      } catch (e) {
        setWorkerOnline(false);
        setEstados(ESTADOS);
      } finally {
        setCheckingWorker(false);
      }
    };
    checkWorker();
    const timer = setInterval(checkWorker, 5000);
    return () => clearInterval(timer);
  }, []);

  // Carrega cidades quando UF muda
  useEffect(() => {
    if (!uf) return;
    fetch(`/api/cidades?uf=${uf}`)
      .then(async (r) => {
        if (!r.ok) throw new Error(`HTTP ${r.status}`);
        return r.json();
      })
      .then((data: Cidade[]) => {
        setCidades(Array.isArray(data) ? data.sort((a, b) => a.nome.localeCompare(b.nome)) : []);
        setCidade("");
        setCidadeId("");
      })
      .catch(() => {
        // Se o worker ainda estiver subindo, o intervalo de verificação refaz a busca.
        setCidades([]);
        setCidade("");
        setCidadeId("");
      });
  }, [uf, workerOnline]);

  // Auto-scroll logs
  useEffect(() => {
    logsEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [logs]);

  // Polling de status
  useEffect(() => {
    if (!jobId || status === "CONCLUIDO" || status === "ERRO") {
      if (pollingRef.current) clearInterval(pollingRef.current);
      return;
    }
    pollingRef.current = setInterval(async () => {
      try {
        const res  = await fetch(`${WORKER_DIRECT}/api/status/${jobId}`);
        if (res.status === 404) {
          setStatus("ERRO");
          setLogs(l => [...l, "[Worker] Job não encontrado — o container reiniciou. Tente novamente."]);
          return;
        }
        const data = await res.json();
        if (data.status) setStatus(data.status);
        setProgresso(data.progresso ?? 0);
        setTotal(data.total ?? 15);
        if (data.logs?.length > 0) setLogs(data.logs);
        if (data.status === "CONCLUIDO") {
          setDownloadUrl(`${WORKER_DIRECT}/api/download/${jobId}`);
        }
      } catch (e) {
        // Worker ainda inicializando ou dormindo — não altera o estado
        console.warn("[Polling] falhou:", e);
      }
    }, 2000);
    return () => { if (pollingRef.current) clearInterval(pollingRef.current); };
  }, [jobId, status]);

  // Drag & drop
  const onDragOver  = useCallback((e: React.DragEvent) => { e.preventDefault(); setDragging(true); }, []);
  const onDragLeave = useCallback(() => setDragging(false), []);
  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault(); setDragging(false);
    const dropped = e.dataTransfer.files[0];
    if (dropped) setFile(dropped);
  }, []);

  const [erroValidacao, setErroValidacao] = useState<string | null>(null);

  const handleIniciar = async () => {
    setErroValidacao(null);
    if (!file)   { setErroValidacao("Selecione o arquivo .FDB/.FBK primeiro."); return; }
    if (!uf)     { setErroValidacao("Selecione o Estado (UF) da empresa."); return; }
    if (!cidade) { setErroValidacao("Selecione a Cidade da empresa."); return; }

    setLogs([]); setProgresso(0); setDownloadUrl(null);
    setStatus("PENDENTE"); setJobId(null);

    const fileMB = (file.size / 1024 / 1024).toFixed(1);
    setLogs([`[Portal] Acordando o servidor...`]);

    // Aquece o container antes do upload (evita cold-start durante o envio)
    try {
      await fetch(`${WORKER_DIRECT}/health`, { signal: AbortSignal.timeout(10000) });
    } catch (_) { /* ignora — tenta o upload mesmo assim */ }

    // Timeout de 10 minutos para arquivos grandes
    const controller = new AbortController();
    const timeoutId  = setTimeout(() => controller.abort(), 600_000);

    const CHUNK_SIZE = 50 * 1024 * 1024; // 50 MB por chunk
    const useChunks  = file.size > 80 * 1024 * 1024;

    try {
      let data: any;

      if (useChunks) {
        // Upload em chunks para arquivos > 80 MB (evita limite de 100 MB do Render/Cloudflare)
        const jobId       = crypto.randomUUID();
        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
        setLogs([`[Portal] Enviando ${file.name} (${fileMB} MB) em ${totalChunks} parte(s)...`]);

        for (let i = 0; i < totalChunks; i++) {
          const start    = i * CHUNK_SIZE;
          const end      = Math.min(start + CHUNK_SIZE, file.size);
          const chunk    = file.slice(start, end);
          const chunkMB  = ((end - start) / 1024 / 1024).toFixed(0);
          setLogs(l => [
            ...l.filter(x => !x.startsWith("[Portal] Enviando parte")),
            `[Portal] Enviando parte ${i + 1}/${totalChunks} (${chunkMB} MB)...`,
          ]);

          const chunkRes = await fetch(`${WORKER_DIRECT}/api/chunk/${jobId}/${i}`, {
            method:  "POST",
            body:    chunk,
            headers: { "Content-Type": "application/octet-stream" },
            signal:  controller.signal,
          });
          if (!chunkRes.ok) throw new Error(`Chunk ${i + 1} falhou: HTTP ${chunkRes.status}`);
        }

        setLogs(l => [...l, "[Portal] Finalizando e iniciando migração..."]);
        const finRes = await fetch(`${WORKER_DIRECT}/api/finalize/${jobId}`, {
          method:  "POST",
          headers: { "Content-Type": "application/json" },
          body:    JSON.stringify({ sistema, uf, cidade, regime, filename: file.name }),
          signal:  controller.signal,
        });
        data = await finRes.json();

      } else {
        setLogs([`[Portal] Enviando arquivo ${file.name} (${fileMB} MB)... aguarde`]);
        const formData = new FormData();
        formData.append("sistema", sistema);
        formData.append("uf",      uf);
        formData.append("cidade",  cidade);
        formData.append("regime",  regime);
        formData.append("arquivo", file, file.name);

        const res = await fetch(`${WORKER_DIRECT}/api/processar`, {
          method: "POST",
          body:   formData,
          signal: controller.signal,
        });
        data = await res.json();
      }

      clearTimeout(timeoutId);

      if (data.jobId) {
        setJobId(data.jobId);
        setStatus("PROCESSANDO");
        setLogs(l => [...l, `[Portal] Arquivo recebido — job ${data.jobId.slice(0, 8)} iniciado`]);
      } else {
        setStatus("ERRO");
        setLogs(l => [...l, "Erro: " + (data.erro ?? data.error ?? "resposta inválida do worker")]);
      }
    } catch (err: any) {
      clearTimeout(timeoutId);
      setStatus("ERRO");
      if (err?.name === "AbortError") {
        setLogs(l => [...l,
          "⚠ Timeout: o arquivo demorou mais de 10 min para ser enviado.",
          "Verifique sua conexão e tente novamente.",
        ]);
      } else {
        setLogs(l => [...l,
          "Não foi possível conectar ao Worker Java.",
          "Worker URL: " + WORKER_DIRECT,
          String(err),
        ]);
      }
    }
  };

  const pct       = total > 0 ? Math.round((progresso / total) * 100) : 0;
  const isRunning = status === "PROCESSANDO" || status === "PENDENTE";

  const statusDot: Record<JobStatus, string> = {
    idle:        "bg-neutral-500",
    PENDENTE:    "bg-yellow-500 animate-pulse",
    PROCESSANDO: "bg-blue-500 animate-pulse",
    CONCLUIDO:   "bg-green-500",
    ERRO:        "bg-red-500",
  };
  const statusLabel: Record<JobStatus, string> = {
    idle:        "Aguardando",
    PENDENTE:    "Pendente",
    PROCESSANDO: "Processando",
    CONCLUIDO:   "Concluído",
    ERRO:        "Erro",
  };



  return (
    <div className="min-h-screen bg-neutral-950 text-white flex flex-col items-center justify-center p-6 relative overflow-hidden">
      <div className="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] rounded-full bg-blue-600/20 blur-[120px] pointer-events-none" />
      <div className="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] rounded-full bg-purple-600/20 blur-[120px] pointer-events-none" />

      <main className="z-10 bg-neutral-900/60 backdrop-blur-xl border border-neutral-800 p-8 rounded-3xl shadow-2xl w-full max-w-2xl flex flex-col gap-6">

        <header className="text-center space-y-1">
          <h1 className="text-4xl font-extrabold tracking-tight bg-gradient-to-r from-blue-400 to-purple-500 bg-clip-text text-transparent">
            Portal de Migração LC
          </h1>
          <div className="flex flex-col items-center gap-1">
             <p className="text-neutral-400 text-sm">Geração Direta de .SQL — Ambiente Cloud</p>
             <div className="flex items-center gap-2 px-3 py-1 bg-neutral-950/50 rounded-full border border-neutral-800">
               <span className={`w-2 h-2 rounded-full ${workerOnline ? "bg-green-500 shadow-[0_0_8px_rgba(34,197,94,0.6)]" : "bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]"}`} />
               <span className="text-[10px] uppercase font-bold tracking-widest text-neutral-500">
                 Worker: {checkingWorker ? "Verificando..." : workerOnline ? "Online" : "Offline"}
               </span>
             </div>
          </div>
        </header>

        {/* Sistema */}
        <div className="space-y-2">
          <label className="text-sm font-semibold text-neutral-300">Sistema de Origem</label>
          <select value={sistema} onChange={e => setSistema(e.target.value)} disabled={isRunning}
            className="w-full bg-neutral-950 border border-neutral-800 text-white px-4 py-3 rounded-xl focus:ring-2 focus:ring-blue-500 outline-none appearance-none disabled:opacity-50">
            {SISTEMAS.map(s => <option key={s.value} value={s.value}>{s.label}</option>)}
          </select>
        </div>

        {/* Estado + Cidade */}
        <div className="grid grid-cols-2 gap-4">
          <div className="space-y-2">
            <label className="text-sm font-semibold text-neutral-300">Estado (UF) <span className="text-red-400">*</span></label>
            <select value={uf} onChange={e => { setUf(e.target.value); setCidades([]); setCidade(""); setCidadeId(""); }} disabled={isRunning}
              className="w-full bg-neutral-950 border border-neutral-800 text-white px-4 py-3 rounded-xl focus:ring-2 focus:ring-blue-500 outline-none appearance-none disabled:opacity-50">
              <option value="">Selecione...</option>
              {estados.map(e => <option key={e.id} value={e.uf}>{e.uf} — {e.nome}</option>)}
            </select>
          </div>
          <div className="space-y-2">
            <label className="text-sm font-semibold text-neutral-300">Cidade <span className="text-red-400">*</span></label>
            <select value={cidade} onChange={e => { setCidade(e.target.value); }} disabled={isRunning || !uf}
              className="w-full bg-neutral-950 border border-neutral-800 text-white px-4 py-3 rounded-xl focus:ring-2 focus:ring-blue-500 outline-none appearance-none disabled:opacity-50">
              <option value="">{uf ? "Selecione..." : "Escolha o estado"}</option>
              {cidades.map(c => <option key={c.id} value={c.nome}>{c.nome}</option>)}
            </select>
          </div>
        </div>

        {/* Regime Tributário */}
        <div className="space-y-2">
          <label className="text-sm font-semibold text-neutral-300">Regime Tributário</label>
          <div className="flex gap-4">
            {[
              { value: "SIMPLES", label: "Simples Nacional" },
              { value: "NORMAL",  label: "Regime Normal" },
            ].map(r => (
              <label key={r.value}
                className={`flex-1 flex items-center justify-center gap-2 py-3 rounded-xl border cursor-pointer transition-all
                  ${isRunning ? "opacity-50 cursor-not-allowed" : ""}
                  ${regime === r.value
                    ? "border-blue-500 bg-blue-500/10 text-blue-300"
                    : "border-neutral-700 hover:border-neutral-500 text-neutral-400"}`}>
                <input type="radio" name="regime" value={r.value} checked={regime === r.value}
                  onChange={() => !isRunning && setRegime(r.value)} className="hidden" />
                <span className={`w-3 h-3 rounded-full border-2 ${regime === r.value ? "border-blue-400 bg-blue-400" : "border-neutral-500"}`} />
                <span className="text-sm font-medium">{r.label}</span>
              </label>
            ))}
          </div>
        </div>

        {/* Drag & Drop */}
        <div className="space-y-2">
          <label className="text-sm font-semibold text-neutral-300">Banco de Dados de Origem</label>
          <div
            onDragOver={onDragOver} onDragLeave={onDragLeave} onDrop={onDrop}
            onClick={() => !isRunning && inputRef.current?.click()}
            className={`border-2 border-dashed rounded-2xl p-10 flex flex-col items-center justify-center cursor-pointer transition-all
              ${isRunning ? "opacity-50 cursor-not-allowed" : ""}
              ${dragging ? "border-blue-400 bg-blue-500/10"
                : file ? "border-green-500/60 bg-green-500/5 hover:border-green-400"
                : "border-neutral-700 hover:border-blue-500 hover:bg-neutral-800/50"}`}
          >
            <input ref={inputRef} type="file" accept=".fdb,.fbk" className="hidden" disabled={isRunning}
              onChange={e => e.target.files?.[0] && setFile(e.target.files[0])} />
            <div className="flex flex-col items-center gap-3 pointer-events-none">
              {file ? (
                <>
                  <svg className="w-10 h-10 text-green-400" fill="none" strokeWidth="1.5" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 12.75L11.25 15 15 9.75M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                  </svg>
                  <p className="text-sm font-semibold text-green-400">{file.name}</p>
                  <p className="text-xs text-neutral-500">{(file.size / 1024 / 1024).toFixed(1)} MB — clique para trocar</p>
                </>
              ) : (
                <>
                  <svg className="w-10 h-10 text-neutral-400" fill="none" strokeWidth="1.5" stroke="currentColor" viewBox="0 0 24 24">
                    <path strokeLinecap="round" strokeLinejoin="round" d="M12 16.5V9.75m0 0l3 3m-3-3l-3 3M6.75 19.5a4.5 4.5 0 01-1.41-8.775 5.25 5.25 0 0110.233-2.33 3 3 0 013.758 3.848A3.752 3.752 0 0118 19.5H6.75z" />
                  </svg>
                  <p className="text-sm font-medium text-neutral-300">Clique ou arraste o arquivo aqui</p>
                  <p className="text-xs text-neutral-500">Arquivos suportados: .FDB ou .FBK</p>
                </>
              )}
            </div>
          </div>
        </div>

        {/* Progresso */}
        {status !== "idle" && (
          <div className="space-y-1">
            <div className="flex justify-between text-xs text-neutral-400">
              <span>Step {progresso} de {total}</span>
              <span>{pct}%</span>
            </div>
            <div className="h-2 bg-neutral-800 rounded-full overflow-hidden">
              <div className={`h-full rounded-full transition-all duration-500 ${
                status === "ERRO" ? "bg-red-500" : status === "CONCLUIDO" ? "bg-green-500" : "bg-blue-500"
              }`} style={{ width: `${pct}%` }} />
            </div>
          </div>
        )}

        {/* Botão e Erros */}
        <div className="space-y-3">
          {erroValidacao && (
            <div className="p-3 rounded-lg bg-red-500/10 border border-red-500/50 text-red-400 text-sm font-medium text-center shadow-inner">
              {erroValidacao}
            </div>
          )}
          <button onClick={handleIniciar} disabled={isRunning || !file}
            className="w-full py-4 bg-gradient-to-r from-blue-600 to-purple-600 hover:from-blue-500 hover:to-purple-500 active:scale-[0.98] disabled:opacity-50 disabled:cursor-not-allowed transition-all transform rounded-xl font-bold text-white shadow-lg shadow-blue-500/25">
            {isRunning ? "Processando..." : "Começar Processamento"}
          </button>
        </div>

        {/* Logs */}
        <div className="bg-neutral-950 rounded-xl border border-neutral-800 overflow-hidden">
          <div className="px-4 py-2 border-b border-neutral-800 flex items-center gap-2">
            <span className={`w-2 h-2 rounded-full ${statusDot[status]}`} />
            <span className="text-xs font-bold text-neutral-500 uppercase tracking-wider">
              Logs da Operação — {statusLabel[status]}
            </span>
          </div>
          <div className="p-4 h-48 overflow-y-auto font-mono text-xs space-y-1">
            {logs.length === 0
              ? <p className="text-neutral-600">Aguardando início da migração...</p>
              : logs.map((l, i) => (
                  <p key={i} className={
                    l.includes("FALHOU") || l.includes("ERRO") || l.includes("✗") ? "text-red-400" :
                    l.includes("sucesso") || l.includes("✓") || l.includes("OK") ? "text-green-400" :
                    l.includes("[Step") ? "text-blue-300" : "text-neutral-300"
                  }>{l}</p>
                ))
            }
            <div ref={logsEndRef} />
          </div>
        </div>

        {/* Download */}
        {downloadUrl && (
          <a href={downloadUrl} download="TabelasParaImportacao.sql"
            className="w-full py-4 bg-gradient-to-r from-green-600 to-emerald-600 hover:from-green-500 hover:to-emerald-500 active:scale-[0.98] transition-all transform rounded-xl font-bold text-white shadow-lg shadow-green-500/25 text-center flex items-center justify-center gap-2 animate-bounce-subtle">
            <svg className="w-5 h-5" fill="none" strokeWidth="2" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M7.5 12L12 16.5m0 0L16.5 12M12 16.5V3" />
            </svg>
            Baixar TabelasParaImportacao.sql
          </a>
        )}

      </main>
    </div>
  );
}
