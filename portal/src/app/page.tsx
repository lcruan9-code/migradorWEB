"use client";

import { useState, useRef, useEffect, useCallback, useMemo } from "react";

/* ─── Types ─────────────────────────────────────────────────── */
type JobStatus = "idle" | "PENDENTE" | "PROCESSANDO" | "CONCLUIDO" | "ERRO";
type Estado    = { id: number; uf: string; nome: string; iduf: string };
type Cidade    = { id: number; nome: string };

/* ─── Design tokens ──────────────────────────────────────────── */
const C = {
  bg0:       "#07060d",
  bg1:       "#0d0b17",
  bg2:       "#141127",
  card:      "#15122a",
  card2:     "#1b1736",
  line:      "#2a2446",
  line2:     "#3a2f63",
  text:      "#e8e6f5",
  textDim:   "#a49fc7",
  textMute:  "#6e6895",
  violet:    "#8b5cf6",
  indigo:    "#6366f1",
  cyan:      "#22d3ee",
  pink:      "#ec4899",
  green:     "#22c55e",
  red:       "#ef4444",
  amber:     "#f59e0b",
  grad:      "linear-gradient(90deg,#7c3aed 0%,#6366f1 50%,#22d3ee 100%)",
  gradSoft:  "linear-gradient(90deg, rgba(124,58,237,.18), rgba(34,211,238,.18))",
} as const;

const inputStyle: React.CSSProperties = {
  width: "100%", background: C.bg0, border: `1px solid ${C.line}`,
  color: C.text, padding: "10px 12px", borderRadius: 10,
  fontSize: 13, outline: "none", fontFamily: "inherit",
};

/* ─── Sistemas ───────────────────────────────────────────────── */
const SISTEMAS = [
  { value: "syspdv", label: "Syspdv (.FDB)"       },
  { value: "gdoor",  label: "GDoor (.FDB)"         },
  { value: "host",   label: "Host (.FDB)"          },
  { value: "clipp",  label: "Clipp (.FBK / .FDB)"  },
];

/* ─── Estados ────────────────────────────────────────────────── */
const ESTADOS_DEFAULT: Estado[] = [
  { id: 1,  uf: "AM", nome: "AMAZONAS",            iduf: "13" },
  { id: 2,  uf: "AP", nome: "AMAPA",               iduf: "16" },
  { id: 3,  uf: "AC", nome: "ACRE",                iduf: "12" },
  { id: 4,  uf: "AL", nome: "ALAGOAS",             iduf: "27" },
  { id: 5,  uf: "BA", nome: "BAHIA",               iduf: "29" },
  { id: 6,  uf: "CE", nome: "CEARA",               iduf: "23" },
  { id: 7,  uf: "DF", nome: "DISTRITO FEDERAL",    iduf: "53" },
  { id: 8,  uf: "ES", nome: "ESPIRITO SANTO",      iduf: "32" },
  { id: 9,  uf: "GO", nome: "GOIAS",               iduf: "52" },
  { id: 10, uf: "MA", nome: "MARANHAO",            iduf: "21" },
  { id: 11, uf: "MG", nome: "MINAS GERAIS",        iduf: "31" },
  { id: 12, uf: "MS", nome: "MATO GROSSO DO SUL",  iduf: "50" },
  { id: 13, uf: "MT", nome: "MATO GROSSO",         iduf: "51" },
  { id: 14, uf: "PA", nome: "PARA",                iduf: "15" },
  { id: 15, uf: "PB", nome: "PARAIBA",             iduf: "25" },
  { id: 16, uf: "PE", nome: "PERNAMBUCO",          iduf: "26" },
  { id: 17, uf: "PR", nome: "PARANA",              iduf: "41" },
  { id: 18, uf: "PI", nome: "PIAUI",               iduf: "22" },
  { id: 19, uf: "RJ", nome: "RIO DE JANEIRO",      iduf: "33" },
  { id: 20, uf: "RN", nome: "RIO GRANDE DO NORTE", iduf: "24" },
  { id: 21, uf: "RS", nome: "RIO GRANDE DO SUL",   iduf: "43" },
  { id: 22, uf: "RO", nome: "RONDONIA",            iduf: "11" },
  { id: 23, uf: "RR", nome: "RORAIMA",             iduf: "14" },
  { id: 24, uf: "SP", nome: "SAO PAULO",           iduf: "35" },
  { id: 25, uf: "SE", nome: "SERGIPE",             iduf: "28" },
  { id: 26, uf: "SC", nome: "SANTA CATARINA",      iduf: "42" },
  { id: 27, uf: "TO", nome: "TOCANTINS",           iduf: "17" },
  { id: 28, uf: "EX", nome: "EXTERIOR",            iduf: "99" },
];

/* ─── Worker URL ─────────────────────────────────────────────── */
const WORKER_DIRECT =
  (process.env.NEXT_PUBLIC_WORKER_URL ?? "http://localhost:8080").replace(/\/$/, "");

/* ═══════════════════════════════════════════════════════════════
   LÓGICA DE SELEÇÃO DE TABELAS
════════════════════════════════════════════════════════════════ */
interface GroupDef {
  id:           string;
  label:        string;
  hint:         string;
  color:        string;
  tables:       string[];
  requires:     string[];
  attractedBy?: string[];
}

const GROUPS: Record<string, GroupDef> = {
  produto: {
    id: "produto", label: "Produtos", hint: "Cadastros principais", color: "#8b5cf6",
    tables: ["PRODUTO","UNIDADE","CATEGORIA","SUBCATEGORIA","FABRICANTE","CST","NCM","CEST","GRUPO_TRIBUTACAO"],
    requires: [],
  },
  fornecedores: {
    id: "fornecedores", label: "Fornecedores", hint: "Cadastro de fornecedor", color: "#a78bfa",
    tables: ["FORNECEDORES"],
    requires: [], attractedBy: ["produto"],
  },
  estoque: {
    id: "estoque", label: "Estoque", hint: "Saldos e movimentos", color: "#22d3ee",
    tables: ["AJUSTE_ESTOQUE","ESTOQUE_SALDO","ESTOQUE"],
    requires: ["produto"],
  },
  cliente: {
    id: "cliente", label: "Clientes", hint: "Base de cadastro", color: "#6366f1",
    tables: ["CLIENTE"],
    requires: [],
  },
  receber: {
    id: "receber", label: "Receber", hint: "Contas a receber (CA)", color: "#22c55e",
    tables: ["RECEBER"],
    requires: ["cliente"],
  },
  pagar: {
    id: "pagar", label: "Pagar", hint: "", color: "#f59e0b",
    tables: ["PAGAR"],
    requires: ["fornecedores"],
  },
};

const COLUMNS = [
  {
    title: "CADASTROS PRINCIPAIS",
    blocks: [
      { kind: "group" as const, id: "produto",
        items: ["PRODUTO","UNIDADE","CATEGORIA","SUBCATEGORIA","FABRICANTE"],
        children: [{ kind: "group" as const, id: "fornecedores" }] },
      { kind: "group" as const, id: "cliente" },
    ],
  },
  {
    title: "TRIBUTAÇÃO",
    note:  "arrastado por Produtos",
    blocks: [
      { kind: "derived" as const, fromGroup: "produto",
        items: ["CST","NCM","CEST","GRUPO_TRIBUTACAO"] },
    ],
  },
  {
    title: "ESTOQUE / LOTE",
    blocks: [
      { kind: "group" as const, id: "estoque",
        items: ["ESTOQUE","AJUSTE_ESTOQUE","ESTOQUE_SALDO"] },
    ],
  },
  {
    title: "FINANCEIRO",
    blocks: [
      { kind: "group" as const, id: "receber" },
      { kind: "group" as const, id: "pagar"   },
    ],
  },
];

function resolve(selected: string[]) {
  const sel = new Set(selected);
  let changed = true;
  while (changed) {
    changed = false;
    for (const id of Array.from(sel)) {
      for (const dep of GROUPS[id].requires) {
        if (!sel.has(dep)) { sel.add(dep); changed = true; }
      }
    }
    for (const gid of Object.keys(GROUPS)) {
      const att = GROUPS[gid].attractedBy ?? [];
      if (att.some((a) => sel.has(a)) && !sel.has(gid)) { sel.add(gid); changed = true; }
    }
  }
  const tables = new Set<string>();
  sel.forEach((gid) => GROUPS[gid].tables.forEach((t) => tables.add(t)));
  return { groups: sel, tables };
}

function isAvailable(groupId: string, selSet: Set<string>) {
  return GROUPS[groupId].requires.every((r) => selSet.has(r));
}

/* ═══════════════════════════════════════════════════════════════
   PRIMITIVOS VISUAIS
════════════════════════════════════════════════════════════════ */
function Dot({
  checked, color = C.violet, disabled = false, onClick, dim = false,
}: {
  checked: boolean; color?: string; disabled?: boolean;
  onClick?: (e: React.MouseEvent) => void; dim?: boolean;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      disabled={disabled}
      aria-pressed={checked}
      style={{
        width: 18, height: 18, borderRadius: "50%", padding: 0, position: "relative",
        border: `1.5px solid ${disabled ? "#39335a" : checked ? color : "#4b4370"}`,
        background: checked ? color : "transparent",
        boxShadow: checked ? `0 0 0 3px ${color}22, 0 0 12px ${color}66` : "none",
        cursor: disabled ? "not-allowed" : "pointer",
        transition: "all .15s ease",
        opacity: disabled ? 0.35 : dim ? 0.75 : 1,
        flex: "0 0 auto",
      }}
    >
      {checked && (
        <span style={{
          position: "absolute", inset: 4, borderRadius: "50%",
          background: "#fff", boxShadow: `0 0 6px ${color}`,
        }} />
      )}
    </button>
  );
}

function TableRow({
  name, checked, disabled, derived, onClick, color,
}: {
  name: string; checked: boolean; disabled: boolean;
  derived?: boolean; onClick: () => void; color: string;
}) {
  return (
    <div
      onClick={disabled ? undefined : onClick}
      style={{
        display: "flex", alignItems: "center", gap: 10,
        padding: "8px 10px", borderRadius: 8,
        cursor: disabled ? "not-allowed" : "pointer",
        background: checked ? `${color}11` : "transparent",
        border: `1px solid ${checked ? color + "44" : "transparent"}`,
        transition: "all .12s ease",
        opacity: disabled ? 0.45 : 1,
      }}
      onMouseEnter={(e) => { if (!disabled && !checked) (e.currentTarget as HTMLElement).style.background = "#ffffff06"; }}
      onMouseLeave={(e) => { if (!disabled && !checked) (e.currentTarget as HTMLElement).style.background = "transparent"; }}
    >
      <Dot checked={checked} color={color} disabled={disabled}
        onClick={(ev) => { ev.stopPropagation(); if (!disabled) onClick(); }} dim={derived} />
      <span style={{
        fontSize: 13.5, fontWeight: checked ? 600 : 500, letterSpacing: 0.1,
        color: checked ? "#fff" : "#cfcae8",
        textDecoration: disabled && !checked ? "line-through" : "none",
      }}>{name}</span>
      {derived && (
        <span style={{
          marginLeft: "auto", fontSize: 10, textTransform: "uppercase",
          letterSpacing: 1, color, opacity: 0.85,
          fontFamily: "JetBrains Mono, monospace",
        }}>auto</span>
      )}
    </div>
  );
}

function ColumnCard({ title, note, children }: {
  title: string; note?: string; children: React.ReactNode;
}) {
  return (
    <div style={{
      background: "linear-gradient(180deg,#15122a 0%,#120f24 100%)",
      border: `1px solid ${C.line}`,
      borderRadius: 14, display: "flex", flexDirection: "column",
      minHeight: 340, overflow: "hidden",
    }}>
      <div style={{
        padding: "10px 14px",
        background: "linear-gradient(180deg,#1d1838,#181530)",
        borderBottom: `1px solid ${C.line}`,
      }}>
        <div style={{ fontSize: 11, fontWeight: 700, letterSpacing: 2, color: "#cfcae8", textTransform: "uppercase" }}>
          {title}
        </div>
        {note && <div style={{ fontSize: 10.5, color: "#8b86ad", marginTop: 2, fontStyle: "italic" }}>{note}</div>}
      </div>
      <div style={{ padding: 8, display: "flex", flexDirection: "column", gap: 2, flex: 1 }}>
        {children}
      </div>
    </div>
  );
}

type BlkDef =
  | { kind: "group"; id: string; items?: string[]; children?: BlkDef[] }
  | { kind: "derived"; fromGroup: string; items: string[] };

function GroupBlock({
  blk, selSet, shakeIds, pulseIds, toggleGroup, nested = false,
}: {
  blk: BlkDef; selSet: Set<string>; shakeIds: Set<string>; pulseIds: Set<string>;
  toggleGroup: (id: string) => void; nested?: boolean;
}) {
  if (blk.kind !== "group") return null;
  const g       = GROUPS[blk.id];
  const avail   = isAvailable(blk.id, selSet);
  const checked = selSet.has(blk.id);
  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
      <div
        className={`${shakeIds.has(blk.id) ? "lc-shake" : ""} ${pulseIds.has(blk.id) ? "lc-pulse" : ""}`}
        onClick={() => toggleGroup(blk.id)}
        style={{
          display: "flex", alignItems: "center", gap: 8,
          padding: nested ? "8px 10px" : "10px 10px",
          borderRadius: 8,
          background: checked ? `linear-gradient(90deg, ${g.color}22, transparent)` : "transparent",
          border: `1px solid ${checked ? g.color + "55" : nested ? "#2a244688" : "transparent"}`,
          cursor: "pointer",
          opacity: avail ? 1 : 0.55,
          position: "relative",
        }}
      >
        <Dot checked={checked} color={g.color}
          onClick={(ev) => { ev.stopPropagation(); toggleGroup(blk.id); }} />
        <div style={{ display: "flex", flexDirection: "column", minWidth: 0 }}>
          <div style={{ fontSize: nested ? 13 : 14, fontWeight: 700, color: checked ? "#fff" : C.text, letterSpacing: 0.2 }}>
            {g.label}
          </div>
          <div style={{ fontSize: 10.5, color: "#8b86ad", marginTop: 1 }}>
            {avail
              ? (g.hint ? `${g.tables.length} tabelas · ${g.hint}` : `${g.tables.length} tabelas`)
              : `requer ${g.requires.map((r) => GROUPS[r].label).join(", ")}`}
          </div>
        </div>
        {g.requires.length > 0 && (
          <span style={{
            marginLeft: "auto", fontSize: 9.5, textTransform: "uppercase",
            letterSpacing: 1, color: avail ? g.color : "#6e6895",
            fontFamily: "JetBrains Mono, monospace",
            border: `1px solid ${avail ? g.color + "55" : "#39335a"}`,
            padding: "2px 6px", borderRadius: 999,
            width: 95, textAlign: "center",
          }}>
            req {g.requires.join("+")}
          </span>
        )}
      </div>

      {blk.items && blk.items.length > 0 && (
        <div style={{
          display: "flex", flexDirection: "column", gap: 2,
          paddingLeft: 6, borderLeft: `1px dashed ${g.color}44`, marginLeft: 14,
        }}>
          {blk.items.map((t) => (
            <TableRow key={t}
              name={t.replace(/_/g, " ")}
              checked={checked} disabled={false}
              derived color={g.color}
              onClick={() => toggleGroup(blk.id)}
            />
          ))}
        </div>
      )}

      {blk.children && blk.children.length > 0 && (
        <div style={{ display: "flex", flexDirection: "column", gap: 4, marginTop: 6, paddingLeft: 10 }}>
          {blk.children.map((ch, i) => (
            <GroupBlock key={i} blk={ch} selSet={selSet} shakeIds={shakeIds}
              pulseIds={pulseIds} toggleGroup={toggleGroup} nested />
          ))}
        </div>
      )}
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   EXTRACT SECTION
════════════════════════════════════════════════════════════════ */
function ExtractSection({
  selected, setSelected, disabled,
}: {
  selected: string[]; setSelected: (v: string[]) => void; disabled: boolean;
}) {
  const selSet = useMemo(() => new Set(selected), [selected]);
  const { groups: effective, tables } = useMemo(() => resolve(selected), [selected]);

  const [shakeIds, setShakeIds] = useState(new Set<string>());
  const [pulseIds, setPulseIds] = useState(new Set<string>());
  const shakeTimers = useRef<Record<string, ReturnType<typeof setTimeout>>>({});

  const triggerShake = useCallback((gid: string) => {
    const reqs = GROUPS[gid].requires;
    setShakeIds((p) => { const n = new Set(p); n.add(gid); return n; });
    setPulseIds((p) => { const n = new Set(p); reqs.forEach((r) => n.add(r)); return n; });
    clearTimeout(shakeTimers.current[gid]);
    shakeTimers.current[gid] = setTimeout(() => {
      setShakeIds((p) => { const n = new Set(p); n.delete(gid); return n; });
      setPulseIds((p) => { const n = new Set(p); reqs.forEach((r) => n.delete(r)); return n; });
    }, 900);
  }, []);

  const toggleGroup = useCallback((gid: string) => {
    if (disabled) return;
    if (!isAvailable(gid, selSet)) { triggerShake(gid); return; }
    const next = new Set(selSet);
    if (next.has(gid)) {
      const lockedBy = Object.keys(GROUPS).filter((other) =>
        next.has(other) && (GROUPS[gid].attractedBy ?? []).includes(other)
      );
      if (lockedBy.length > 0) {
        setShakeIds((p) => { const n = new Set(p); n.add(gid); return n; });
        setPulseIds((p) => { const n = new Set(p); lockedBy.forEach((l) => n.add(l)); return n; });
        clearTimeout(shakeTimers.current[gid]);
        shakeTimers.current[gid] = setTimeout(() => {
          setShakeIds((p) => { const n = new Set(p); n.delete(gid); return n; });
          setPulseIds((p) => { const n = new Set(p); lockedBy.forEach((l) => n.delete(l)); return n; });
        }, 900);
        return;
      }
      next.delete(gid);
      for (const other of Object.keys(GROUPS)) {
        if (GROUPS[other].requires.includes(gid)) next.delete(other);
      }
    } else {
      next.add(gid);
      let changed = true;
      while (changed) {
        changed = false;
        for (const other of Object.keys(GROUPS)) {
          const att = GROUPS[other].attractedBy ?? [];
          if (att.some((a) => next.has(a)) && !next.has(other)) { next.add(other); changed = true; }
        }
      }
    }
    setSelected(Array.from(next));
  }, [disabled, selSet, setSelected, triggerShake]);

  const allSelected = Object.keys(GROUPS).every((g) => selSet.has(g));
  const selectAll   = () => !disabled && setSelected(allSelected ? [] : Object.keys(GROUPS));

  return (
    <div style={{ width: "100%", opacity: disabled ? 0.6 : 1, transition: "opacity .3s" }}>
      {/* Header */}
      <div style={{
        background: "linear-gradient(90deg,#0b0914,#15122a 40%,#0b0914)",
        border: `1px solid ${C.line}`, borderBottom: "none",
        borderRadius: "14px 14px 0 0",
        padding: "14px 18px",
        display: "flex", alignItems: "center", justifyContent: "space-between",
      }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{
            width: 28, height: 28, borderRadius: 8,
            background: C.grad, display: "grid", placeItems: "center",
            boxShadow: "0 0 18px rgba(139,92,246,.55)",
          }}>
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
              <ellipse cx="12" cy="5" rx="8" ry="3" />
              <path d="M4 5v6c0 1.66 3.58 3 8 3s8-1.34 8-3V5" />
              <path d="M4 11v6c0 1.66 3.58 3 8 3s8-1.34 8-3v-6" />
            </svg>
          </div>
          <div>
            <div style={{ fontSize: 17, fontWeight: 700, letterSpacing: 0.3 }}>Extrair Tabelas</div>
            <div style={{ fontSize: 11.5, color: "#8b86ad", marginTop: 1 }}>
              Selecione o que será entregue na migração final
            </div>
          </div>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <span style={{
            fontFamily: "JetBrains Mono, monospace", fontSize: 11,
            color: "#cfcae8", background: "#0b0914", border: `1px solid ${C.line}`,
            padding: "5px 10px", borderRadius: 999,
          }}>
            {tables.size} tabelas · {effective.size} grupos
          </span>
          <button onClick={selectAll} disabled={disabled} style={{
            fontSize: 11, fontWeight: 600, letterSpacing: 0.6, textTransform: "uppercase",
            color: "#cfcae8", background: "#0b0914",
            border: `1px solid ${C.line2}`, padding: "6px 12px", borderRadius: 8,
            cursor: disabled ? "not-allowed" : "pointer",
          }}>
            {allSelected ? "Limpar" : "Selecionar tudo"}
          </button>
        </div>
      </div>

      {/* 4 colunas */}
      <div style={{
        background: "#0b0914",
        border: `1px solid ${C.line}`,
        borderRadius: "0 0 14px 14px",
        padding: 14,
        display: "grid",
        gridTemplateColumns: "repeat(4, 1fr)",
        gap: 12,
      }}>
        {COLUMNS.map((col, ci) => (
          <ColumnCard key={ci} title={col.title} note={col.note}>
            {col.blocks.map((blk, bi) => {
              if (blk.kind === "group") {
                return (
                  <GroupBlock key={bi} blk={blk} selSet={selSet}
                    shakeIds={shakeIds} pulseIds={pulseIds} toggleGroup={toggleGroup} />
                );
              }
              if (blk.kind === "derived") {
                const g       = GROUPS[blk.fromGroup];
                const checked = selSet.has(blk.fromGroup);
                return (
                  <div key={bi} style={{ display: "flex", flexDirection: "column", gap: 2 }}>
                    {blk.items.map((t) => (
                      <TableRow key={t}
                        name={t.replace(/_/g, " ")}
                        checked={checked} disabled={false} derived
                        color={g.color}
                        onClick={() => toggleGroup(blk.fromGroup)}
                      />
                    ))}
                  </div>
                );
              }
              return null;
            })}
          </ColumnCard>
        ))}
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   LOG PANEL
════════════════════════════════════════════════════════════════ */
function LogPanel({ logs, status }: { logs: string[]; status: JobStatus }) {
  const ref = useRef<HTMLDivElement>(null);
  useEffect(() => { if (ref.current) ref.current.scrollTop = ref.current.scrollHeight; }, [logs]);

  const dotColor: Record<JobStatus, string> = {
    idle:        C.textMute,
    PENDENTE:    C.amber,
    PROCESSANDO: C.indigo,
    CONCLUIDO:   C.green,
    ERRO:        C.red,
  };
  const dotGlow: Record<JobStatus, string> = {
    idle:        "none",
    PENDENTE:    `0 0 6px ${C.amber}`,
    PROCESSANDO: `0 0 6px ${C.indigo}`,
    CONCLUIDO:   `0 0 6px ${C.green}`,
    ERRO:        `0 0 6px ${C.red}`,
  };
  const statusLabel: Record<JobStatus, string> = {
    idle: "Aguardando", PENDENTE: "Pendente",
    PROCESSANDO: "Processando", CONCLUIDO: "Concluído", ERRO: "Erro",
  };

  return (
    <div style={{
      background: C.bg0, border: `1px solid ${C.line}`, borderRadius: 12,
      padding: 12, display: "flex", flexDirection: "column", gap: 8,
    }}>
      <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
        <span style={{
          width: 7, height: 7, borderRadius: "50%",
          background: dotColor[status], boxShadow: dotGlow[status],
        }} />
        <span style={{ fontSize: 10.5, letterSpacing: 1.5, textTransform: "uppercase", fontWeight: 700, color: "#cfcae8" }}>
          Logs da Operação — {statusLabel[status]}
        </span>
        {(status === "PROCESSANDO" || status === "PENDENTE") && (
          <span style={{ marginLeft: "auto", fontSize: 10, color: C.textMute, fontFamily: "JetBrains Mono, monospace" }}>
            live
          </span>
        )}
      </div>
      <div ref={ref} style={{
        fontFamily: "JetBrains Mono, monospace",
        fontSize: 11.5, lineHeight: 1.55,
        maxHeight: 220, minHeight: 120, overflowY: "auto",
        whiteSpace: "pre-wrap",
      }}>
        {logs.length === 0
          ? <span style={{ color: C.textMute }}>Aguardando início da migração...</span>
          : logs.map((l, i) => (
              <div key={i} style={{
                color:
                  l.includes("FALHOU") || l.includes("ERRO") || l.includes("✗") ? C.red :
                  l.includes("sucesso") || l.includes("✓") || l.includes("OK")  ? C.green :
                  l.includes("[Step") ? "#a5b4fc" : C.textDim,
              }}>
                {l}
              </div>
            ))
        }
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════════════════════════
   FIELD WRAPPER
════════════════════════════════════════════════════════════════ */
function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <span style={{ fontSize: 11.5, color: "#cfcae8", fontWeight: 600, letterSpacing: 0.2 }}>{label}</span>
      {children}
    </label>
  );
}

/* ═══════════════════════════════════════════════════════════════
   PAGE
════════════════════════════════════════════════════════════════ */
export default function Home() {
  /* — form state — */
  const [sistema,  setSistema]  = useState("syspdv");
  const [file,     setFile]     = useState<File | null>(null);
  const [dragging, setDragging] = useState(false);
  const [estados,  setEstados]  = useState<Estado[]>(ESTADOS_DEFAULT);
  const [cidades,  setCidades]  = useState<Cidade[]>([]);
  const [uf,       setUf]       = useState("");
  const [cidade,   setCidade]   = useState("");
  const [regime,   setRegime]   = useState("SIMPLES");
  const [cnpjDestino, setCnpjDestino] = useState("");

  /* — seleção de tabelas — */
  const [selected, setSelected] = useState<string[]>([]);
  const { tables: selectedTables } = useMemo(() => resolve(selected), [selected]);

  /* — worker — */
  const [workerOnline,   setWorkerOnline]   = useState(false);
  const [checkingWorker, setCheckingWorker] = useState(true);

  /* — job — */
  const [jobId,       setJobId]       = useState<string | null>(null);
  const [status,      setStatus]      = useState<JobStatus>("idle");
  const [progresso,   setProgresso]   = useState(0);
  const [total,       setTotal]       = useState(15);
  const [logs,        setLogs]        = useState<string[]>([]);
  const [downloadUrl, setDownloadUrl] = useState<string | null>(null);
  const [erroValidacao, setErroValidacao] = useState<string | null>(null);

  const pollingRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const inputRef   = useRef<HTMLInputElement>(null);

  /* — verifica worker a cada 5s — */
  useEffect(() => {
    const check = async () => {
      try {
        const r = await fetch("/api/estados");
        if (r.ok) {
          const data = await r.json();
          if (data?.length > 0) setEstados(data);
          setWorkerOnline(true);
        } else { setWorkerOnline(false); setEstados(ESTADOS_DEFAULT); }
      } catch { setWorkerOnline(false); setEstados(ESTADOS_DEFAULT); }
      finally  { setCheckingWorker(false); }
    };
    check();
    const t = setInterval(check, 5000);
    return () => clearInterval(t);
  }, []);

  /* — carrega cidades — */
  useEffect(() => {
    if (!uf) return;
    fetch(`/api/cidades?uf=${uf}`)
      .then(async (r) => { if (!r.ok) throw new Error(); return r.json(); })
      .then((data: Cidade[]) => {
        setCidades(Array.isArray(data) ? data.sort((a, b) => a.nome.localeCompare(b.nome)) : []);
        setCidade("");
      })
      .catch(() => { setCidades([]); setCidade(""); });
  }, [uf, workerOnline]);

  /* — polling — */
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
          setLogs((l) => [...l, "[Worker] Job não encontrado — o container reiniciou. Tente novamente."]);
          return;
        }
        const data = await res.json();
        if (data.status)  setStatus(data.status);
        setProgresso(data.progresso ?? 0);
        setTotal(data.total ?? 15);
        if (data.logs?.length > 0) setLogs(data.logs);
        if (data.status === "CONCLUIDO") setDownloadUrl(`${WORKER_DIRECT}/api/download/${jobId}`);
      } catch { /* worker inicializando */ }
    }, 2000);
    return () => { if (pollingRef.current) clearInterval(pollingRef.current); };
  }, [jobId, status]);

  /* — drag & drop — */
  const onDragOver  = useCallback((e: React.DragEvent) => { e.preventDefault(); setDragging(true); }, []);
  const onDragLeave = useCallback(() => setDragging(false), []);
  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault(); setDragging(false);
    const f = e.dataTransfer.files[0];
    if (f) setFile(f);
  }, []);

  /* — iniciar migração — */
  const isRunning = status === "PROCESSANDO" || status === "PENDENTE";

  const handleIniciar = async () => {
    setErroValidacao(null);
    if (!file)   { setErroValidacao("Selecione o arquivo .FDB/.FBK primeiro."); return; }
    if (!uf)     { setErroValidacao("Selecione o Estado (UF) da empresa."); return; }
    if (!cidade) { setErroValidacao("Selecione a Cidade da empresa."); return; }

    setLogs([]); setProgresso(0); setDownloadUrl(null);
    setStatus("PENDENTE"); setJobId(null);

    const fileMB = (file.size / 1024 / 1024).toFixed(1);
    setLogs([`[Portal] Acordando o servidor...`]);

    try {
      await fetch(`${WORKER_DIRECT}/health`, { signal: AbortSignal.timeout(10000) });
    } catch { /* ignora */ }

    const controller = new AbortController();
    const timeoutId  = setTimeout(() => controller.abort(), 600_000);

    const CHUNK_SIZE = 50 * 1024 * 1024;
    const useChunks  = file.size > 80 * 1024 * 1024;

    // extras de payload
    const extras = {
      cnpjDestino:       cnpjDestino.replace(/\D/g, ""),
      gruposSelecionados: selected,
      tabelas:           Array.from(selectedTables),
    };

    try {
      let data: Record<string, unknown>;

      if (useChunks) {
        const jobUuid     = crypto.randomUUID();
        const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
        setLogs([`[Portal] Enviando ${file.name} (${fileMB} MB) em ${totalChunks} parte(s)...`]);

        for (let i = 0; i < totalChunks; i++) {
          const start   = i * CHUNK_SIZE;
          const end     = Math.min(start + CHUNK_SIZE, file.size);
          const chunk   = file.slice(start, end);
          const chunkMB = ((end - start) / 1024 / 1024).toFixed(0);
          setLogs((l) => [
            ...l.filter((x) => !x.startsWith("[Portal] Enviando parte")),
            `[Portal] Enviando parte ${i + 1}/${totalChunks} (${chunkMB} MB)...`,
          ]);
          const chunkRes = await fetch(`${WORKER_DIRECT}/api/chunk/${jobUuid}/${i}`, {
            method: "POST", body: chunk,
            headers: { "Content-Type": "application/octet-stream" },
            signal: controller.signal,
          });
          if (!chunkRes.ok) throw new Error(`Chunk ${i + 1} falhou: HTTP ${chunkRes.status}`);
        }

        setLogs((l) => [...l, "[Portal] Finalizando e iniciando migração..."]);
        const finRes = await fetch(`${WORKER_DIRECT}/api/finalize/${jobUuid}`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ sistema, uf, cidade, regime, filename: file.name, ...extras }),
          signal: controller.signal,
        });
        data = await finRes.json();
      } else {
        setLogs([`[Portal] Enviando arquivo ${file.name} (${fileMB} MB)... aguarde`]);
        const formData = new FormData();
        formData.append("sistema",  sistema);
        formData.append("uf",       uf);
        formData.append("cidade",   cidade);
        formData.append("regime",   regime);
        formData.append("arquivo",  file, file.name);
        if (extras.cnpjDestino)
          formData.append("cnpjDestino", extras.cnpjDestino);
        if (extras.tabelas.length > 0)
          formData.append("tabelas", extras.tabelas.join(","));
        if (extras.gruposSelecionados.length > 0)
          formData.append("gruposSelecionados", extras.gruposSelecionados.join(","));

        const res = await fetch(`${WORKER_DIRECT}/api/processar`, {
          method: "POST", body: formData, signal: controller.signal,
        });
        data = await res.json();
      }

      clearTimeout(timeoutId);

      if (data.jobId) {
        setJobId(data.jobId as string);
        setStatus("PROCESSANDO");
        setLogs((l) => [...l, `[Portal] Arquivo recebido — job ${(data.jobId as string).slice(0, 8)} iniciado`]);
      } else {
        setStatus("ERRO");
        setLogs((l) => [...l, "Erro: " + (data.erro ?? data.error ?? "resposta inválida do worker")]);
      }
    } catch (err: unknown) {
      clearTimeout(timeoutId);
      setStatus("ERRO");
      const e = err as { name?: string };
      if (e?.name === "AbortError") {
        setLogs((l) => [...l,
          "⚠ Timeout: o arquivo demorou mais de 10 min para ser enviado.",
          "Verifique sua conexão e tente novamente.",
        ]);
      } else {
        setLogs((l) => [...l,
          "Não foi possível conectar ao Worker Java.",
          "Worker URL: " + WORKER_DIRECT,
          String(err),
        ]);
      }
    }
  };

  const pct = total > 0 ? Math.round((progresso / total) * 100) : 0;

  const resetForm = () => {
    setStatus("idle"); setJobId(null); setLogs([]);
    setProgresso(0); setDownloadUrl(null); setErroValidacao(null);
    setFile(null); setSelected([]);
  };

  /* ─── CNPJ mask ─────────────────────────────────────────── */
  const handleCnpj = (v: string) => {
    let d = v.replace(/\D/g, "").slice(0, 14);
    d = d
      .replace(/^(\d{2})(\d)/, "$1.$2")
      .replace(/^(\d{2})\.(\d{3})(\d)/, "$1.$2.$3")
      .replace(/\.(\d{3})(\d)/, ".$1/$2")
      .replace(/(\d{4})(\d)/, "$1-$2");
    setCnpjDestino(d);
  };

  /* ─── RENDER ─────────────────────────────────────────────── */
  return (
    <div style={{
      minHeight: "100vh",
      background: `
        radial-gradient(1200px 600px at 85% -10%, rgba(124,58,237,.22), transparent 60%),
        radial-gradient(900px 500px at -10% 110%, rgba(34,211,238,.14), transparent 60%),
        radial-gradient(600px 400px at 50% 120%, rgba(236,72,153,.10), transparent 60%),
        ${C.bg0}
      `,
      padding: "28px 28px 60px",
      display: "grid",
      gridTemplateColumns: "minmax(360px, 440px) 1fr",
      gridTemplateRows: "auto 1fr",
      gap: 24,
      maxWidth: 1440,
      margin: "0 auto",
      boxSizing: "border-box",
    }}>

      {/* ── Header ─────────────────────────────────────────── */}
      <div style={{ gridColumn: "1 / -1", display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 4 }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
          <div style={{
            width: 34, height: 34, borderRadius: 10,
            background: C.grad, display: "grid", placeItems: "center",
            boxShadow: "0 0 22px rgba(99,102,241,.45)",
          }}>
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="#fff" strokeWidth="2.2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M7 10l5-5 5 5" /><path d="M7 14l5 5 5-5" />
            </svg>
          </div>
          <div>
            <div style={{ fontSize: 14, fontWeight: 700, letterSpacing: 0.3 }}>LC Portal — Migrador Web</div>
            <div style={{ fontSize: 11, color: "#8b86ad" }}>Geração Direta de SQL — Ambiente Cloud</div>
          </div>
        </div>
        <div style={{ display: "inline-flex", alignItems: "center", gap: 6,
          background: "#0b0914", border: `1px solid ${workerOnline ? "#1f3a2a" : "#3a1f1f"}`, borderRadius: 999,
          padding: "4px 10px" }}>
          <span style={{
            width: 7, height: 7, borderRadius: "50%",
            background: workerOnline ? C.green : C.red,
            boxShadow: `0 0 8px ${workerOnline ? C.green : C.red}`,
          }} />
          <span style={{ fontSize: 10.5, color: workerOnline ? C.green : C.red, fontWeight: 600, letterSpacing: 1, textTransform: "uppercase" as const }}>
            Worker: {checkingWorker ? "Verificando..." : workerOnline ? "Online" : "Offline"}
          </span>
        </div>
      </div>

      {/* ── Coluna esquerda ─────────────────────────────────── */}
      <div style={{ display: "flex", flexDirection: "column", gap: 16 }}>

        {/* Origin panel */}
        <div style={{
          background: "linear-gradient(180deg, rgba(21,18,42,.85), rgba(13,11,23,.85))",
          border: `1px solid ${C.line}`,
          borderRadius: 16, padding: 20, backdropFilter: "blur(8px)",
          display: "flex", flexDirection: "column", gap: 14,
        }}>
          {/* Título interno */}
          <div style={{ textAlign: "center" }}>
            <div style={{
              background: C.grad, WebkitBackgroundClip: "text", backgroundClip: "text",
              color: "transparent", fontSize: 24, fontWeight: 800, letterSpacing: 0.2,
            }}>
              Portal de Migração LC
            </div>
            <div style={{ fontSize: 11.5, color: "#8b86ad", marginTop: 3 }}>
              Geração Direta de .SQL — Ambiente Cloud
            </div>
          </div>

          {/* Sistema */}
          <Field label="Sistema de Origem">
            <select value={sistema} onChange={(e) => setSistema(e.target.value)}
              disabled={isRunning} style={inputStyle}>
              {SISTEMAS.map((s) => <option key={s.value} value={s.value}>{s.label}</option>)}
            </select>
          </Field>

          {/* Estado + Cidade */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 10 }}>
            <Field label="Estado (UF) *">
              <select value={uf} disabled={isRunning}
                onChange={(e) => { setUf(e.target.value); setCidades([]); setCidade(""); }}
                style={inputStyle}>
                <option value="">Selecione...</option>
                {estados.map((e) => <option key={e.id} value={e.uf}>{e.uf} — {e.nome}</option>)}
              </select>
            </Field>
            <Field label="Cidade *">
              <select value={cidade} disabled={isRunning || !uf}
                onChange={(e) => setCidade(e.target.value)}
                style={inputStyle}>
                <option value="">{uf ? "Selecione..." : "Escolha o estado"}</option>
                {cidades.map((c) => <option key={c.id} value={c.nome}>{c.nome}</option>)}
              </select>
            </Field>
          </div>

          {/* Regime */}
          <Field label="Regime Tributário">
            <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 8 }}>
              {[{ k: "SIMPLES", n: "Simples Nacional" }, { k: "NORMAL", n: "Regime Normal" }].map((opt) => {
                const on = regime === opt.k;
                return (
                  <div key={opt.k} onClick={() => !isRunning && setRegime(opt.k)} style={{
                    background: on ? "linear-gradient(180deg, rgba(99,102,241,.24), rgba(99,102,241,.08))" : "#0b0914",
                    border: `1px solid ${on ? C.indigo : C.line}`,
                    color: on ? "#fff" : "#cfcae8",
                    padding: "10px 12px", borderRadius: 10,
                    cursor: isRunning ? "not-allowed" : "pointer",
                    fontSize: 13, fontWeight: 500,
                    display: "flex", alignItems: "center", gap: 8,
                    boxShadow: on ? "0 0 0 3px rgba(99,102,241,.15)" : "none",
                    userSelect: "none" as const,
                    opacity: isRunning ? 0.6 : 1,
                  }}>
                    <Dot checked={on} color={C.indigo} onClick={() => !isRunning && setRegime(opt.k)} />
                    {opt.n}
                  </div>
                );
              })}
            </div>
          </Field>

          {/* CNPJ Destino */}
          <Field label="CNPJ de Destino">
            <input
              value={cnpjDestino}
              onChange={(e) => handleCnpj(e.target.value)}
              placeholder="00.000.000/0000-00"
              inputMode="numeric"
              disabled={isRunning}
              style={{ ...inputStyle, fontFamily: "JetBrains Mono, monospace", letterSpacing: 0.4 }}
            />
            <span style={{ fontSize: 10.5, color: "#8b86ad", marginTop: -2 }}>
              Aplicado na tabela <b style={{ color: "#cfcae8" }}>EMPRESA</b> ao final da migração
            </span>
          </Field>

          {/* Upload */}
          <Field label="Banco de Dados de Origem">
            <div
              onDragOver={onDragOver} onDragLeave={onDragLeave} onDrop={onDrop}
              onClick={() => !isRunning && inputRef.current?.click()}
              style={{
                background: file
                  ? "linear-gradient(180deg, rgba(34,197,94,.12), rgba(34,197,94,.04))"
                  : dragging
                    ? "rgba(99,102,241,.08)"
                    : "#0b0914",
                border: `1px dashed ${file ? "#22c55e66" : dragging ? C.indigo : C.line}`,
                borderRadius: 12, padding: "16px 12px",
                textAlign: "center", cursor: isRunning ? "not-allowed" : "pointer",
                transition: "all .2s ease",
                opacity: isRunning ? 0.7 : 1,
              }}
            >
              <input ref={inputRef} type="file" accept=".fdb,.fbk" className="hidden"
                disabled={isRunning}
                onChange={(e) => e.target.files?.[0] && setFile(e.target.files[0])} />
              {file ? (
                <>
                  <div style={{
                    margin: "0 auto 8px", width: 38, height: 38, borderRadius: "50%",
                    background: "rgba(34,197,94,.18)", display: "grid", placeItems: "center",
                    boxShadow: "0 0 18px rgba(34,197,94,.35)",
                  }}>
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#22c55e" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M20 6 9 17l-5-5" />
                    </svg>
                  </div>
                  <div style={{ fontFamily: "JetBrains Mono, monospace", fontSize: 13, color: C.green, fontWeight: 600 }}>
                    {file.name}
                  </div>
                  <div style={{ fontSize: 10.5, color: "#8b86ad", marginTop: 3 }}>
                    {(file.size / 1024 / 1024).toFixed(1)} MB — clique para trocar
                  </div>
                </>
              ) : (
                <>
                  <div style={{
                    margin: "0 auto 8px", width: 38, height: 38, borderRadius: "50%",
                    background: "rgba(99,102,241,.12)", display: "grid", placeItems: "center",
                  }}>
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke={C.textDim} strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round">
                      <path d="M12 16.5V9.75m0 0l3 3m-3-3l-3 3M6.75 19.5a4.5 4.5 0 01-1.41-8.775 5.25 5.25 0 0110.233-2.33 3 3 0 013.758 3.848A3.752 3.752 0 0118 19.5H6.75z" />
                    </svg>
                  </div>
                  <div style={{ fontSize: 13, fontWeight: 500, color: "#cfcae8" }}>Clique ou arraste o arquivo aqui</div>
                  <div style={{ fontSize: 10.5, color: "#8b86ad", marginTop: 2 }}>Arquivos suportados: .FDB ou .FBK</div>
                </>
              )}
            </div>
          </Field>

          {/* Progress bar */}
          {status !== "idle" && (
            <div>
              <div style={{ display: "flex", justifyContent: "space-between", fontSize: 11, color: "#8b86ad", marginBottom: 6 }}>
                <span>Step {progresso} de {total}</span>
                <span>{pct}%</span>
              </div>
              <div style={{ height: 6, background: C.bg2, borderRadius: 999, overflow: "hidden", border: `1px solid ${C.line}` }}>
                <div style={{
                  height: "100%",
                  width: `${pct}%`,
                  background: status === "ERRO" ? C.red : status === "CONCLUIDO" ? C.green : C.grad,
                  boxShadow: status === "PROCESSANDO" ? "0 0 12px rgba(139,92,246,.5)" : "none",
                  transition: "width .4s ease",
                }} />
              </div>
            </div>
          )}

          {/* Erro validação */}
          {erroValidacao && (
            <div style={{
              padding: "10px 12px", borderRadius: 8,
              background: "rgba(239,68,68,.1)", border: "1px solid rgba(239,68,68,.4)",
              color: C.red, fontSize: 13, fontWeight: 500,
            }}>
              {erroValidacao}
            </div>
          )}

          {/* Botão */}
          {(status === "CONCLUIDO" || status === "ERRO") ? (
            <button onClick={resetForm} style={{
              background: "transparent", border: `1px solid ${C.line2}`,
              borderRadius: 10, padding: "13px 16px",
              color: "#cfcae8", fontWeight: 600, fontSize: 13,
              cursor: "pointer", letterSpacing: 0.3,
            }}>
              ↺ Nova Migração
            </button>
          ) : (
            <button onClick={handleIniciar} disabled={isRunning || !file} style={{
              background: isRunning || !file ? "rgba(99,102,241,.3)" : C.grad,
              border: "none", borderRadius: 10, padding: "13px 16px",
              color: "#fff", fontWeight: 700, letterSpacing: 0.3, fontSize: 14,
              cursor: isRunning || !file ? "not-allowed" : "pointer",
              boxShadow: isRunning || !file ? "none" : "0 0 22px rgba(124,58,237,.35)",
              transition: "all .2s ease",
            }}>
              {isRunning ? "Processando migração…" : "Começar Processamento"}
            </button>
          )}

          {/* Download */}
          {downloadUrl && (
            <a href={downloadUrl} download="TabelasParaImportacao.sql"
              className="animate-bounce-subtle"
              style={{
                background: "linear-gradient(90deg, #16a34a, #059669)",
                borderRadius: 10, padding: "13px 16px",
                color: "#fff", fontWeight: 700, fontSize: 14, letterSpacing: 0.3,
                textDecoration: "none", textAlign: "center",
                display: "flex", alignItems: "center", justifyContent: "center", gap: 8,
                boxShadow: "0 0 22px rgba(22,163,74,.35)",
              }}>
              <svg width="18" height="18" fill="none" strokeWidth="2" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5M7.5 12L12 16.5m0 0L16.5 12M12 16.5V3" />
              </svg>
              Baixar TabelasParaImportacao.sql
            </a>
          )}
        </div>

        {/* Log panel */}
        <LogPanel logs={logs} status={status} />
      </div>

      {/* ── Coluna direita — Extrair Tabelas ─────────────────── */}
      <div style={{ display: "flex", flexDirection: "column", gap: 16, minWidth: 0 }}>
        <ExtractSection selected={selected} setSelected={setSelected} disabled={isRunning} />
      </div>

    </div>
  );
}
