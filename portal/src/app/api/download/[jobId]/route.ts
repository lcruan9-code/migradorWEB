import { NextRequest, NextResponse } from 'next/server';
import { getWorkerUrl } from '@/lib/worker';

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ jobId: string }> }
) {
  const { jobId } = await params;

  try {
    const response = await fetch(`${getWorkerUrl()}/api/download/${jobId}`);

    if (!response.ok) {
      return NextResponse.json(
        { error: 'Falha ao buscar arquivo no worker' },
        { status: response.status }
      );
    }

    const blob = await response.blob();

    // Retorna o arquivo com os cabeçalhos corretos para forçar o download no Windows
    return new NextResponse(blob, {
      status: 200,
      headers: {
        'Content-Type': 'application/octet-stream',
        'Content-Disposition': 'attachment; filename="TabelasParaImportacao.sql"',
      },
    });
  } catch (error) {
    console.error('Erro no Proxy de Download:', error);
    return NextResponse.json(
      { error: 'Erro interno ao processar o download' },
      { status: 500 }
    );
  }
}
