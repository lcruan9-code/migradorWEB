import { NextRequest, NextResponse } from "next/server";
import { getWorkerUrl } from "@/lib/worker";

export async function GET(
  request: NextRequest,
  { params }: { params: { jobId: string } }
) {
  try {
    const response = await fetch(`${getWorkerUrl()}/api/status/${params.jobId}`);
    const text = await response.text();
    return new NextResponse(text, {
      status: response.status,
      headers: {
        "Content-Type": response.headers.get("content-type") || "application/json",
      },
    });
  } catch (error) {
    return NextResponse.json(
      { error: "Falha ao consultar status no worker" },
      { status: 500 }
    );
  }
}
