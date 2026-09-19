-- Evento complexo de negócio: tomada de conta
SELECT a.clienteId, d.txId, d.valor
FROM PATTERN [
      every a = LoginRealizado(deviceConhecido = false)
   -> b = SenhaAlterada(clienteId = a.clienteId)
   -> c = ChavePixCadastrada(clienteId = a.clienteId)
   -> d = PixEnviado(clienteId = a.clienteId)
] WHERE timer:within(10 minutes)
  AND NOT EXISTS (
      SELECT 1 FROM BiometriaConfirmada(clienteId = a.clienteId).win:time(10 min)
  );

-- Evento complexo técnico: degradação do Pix
SELECT avg(r.latenciaMs) AS latMedia, count(t.*) AS timeouts
FROM RequisicaoAPI(endpoint = '/pix').win:time(2 min) AS r,
     TimeoutIntegracao(integracao = 'SPI').win:time(2 min) AS t
GROUP BY 1
HAVING avg(r.latenciaMs) > 800 AND count(t.*) >= 10;
