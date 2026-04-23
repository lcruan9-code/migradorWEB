import { NextResponse } from "next/server";
import { getWorkerUrl } from "@/lib/worker";

export async function GET() {
  try {
    const response = await fetch(`${getWorkerUrl()}/api/estados`);
    const text = await response.text();
    return new NextResponse(text, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("content-type") || "application/json",
      },
    });
  } catch (error) {
    return NextResponse.json(
      { error: "Falha ao buscar estados no worker" },
      { status: 500 }
    );
  }
}
