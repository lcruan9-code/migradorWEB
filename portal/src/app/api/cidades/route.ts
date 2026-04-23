import { NextRequest, NextResponse } from "next/server";
import { getWorkerUrl } from "@/lib/worker";

export async function GET(request: NextRequest) {
  const uf = request.nextUrl.searchParams.get("uf") || "";

  try {
    const response = await fetch(
      `${getWorkerUrl()}/api/cidades?uf=${encodeURIComponent(uf)}`
    );
    const text = await response.text();
    return new NextResponse(text, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("content-type") || "application/json",
      },
    });
  } catch (error) {
    return NextResponse.json(
      { error: "Falha ao buscar cidades no worker" },
      { status: 500 }
    );
  }
}
