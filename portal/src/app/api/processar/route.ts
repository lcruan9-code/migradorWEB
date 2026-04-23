import { NextRequest, NextResponse } from "next/server";
import { getWorkerUrl } from "@/lib/worker";

export async function POST(request: NextRequest) {
  try {
    const body = await request.arrayBuffer();
    const response = await fetch(`${getWorkerUrl()}/api/processar`, {
      method: "POST",
      headers: {
        "Content-Type": request.headers.get("content-type") || "multipart/form-data",
      },
      body,
    });

    const text = await response.text();
    return new NextResponse(text, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("content-type") || "application/json",
      },
    });
  } catch (error) {
    return NextResponse.json(
      { error: "Falha ao encaminhar processamento ao worker" },
      { status: 500 }
    );
  }
}
