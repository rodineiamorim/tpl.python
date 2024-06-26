import unicodedata
import re

def removeAcentos(palavra):

  # Unicode normalize transforma um caracter em seu equivalente em latin.
  nfkd = unicodedata.normalize('NFKD', palavra)
  palavraSemAcento = u"".join([c for c in nfkd if not unicodedata.combining(c)])

  # Usa expressão regular para retornar a palavra apenas com números, letras e espaço
  return re.sub('[^a-zA-Z0-9.,;/ \\\]', '', palavraSemAcento)

def ocorrenciaPedido(db, idcliente, pedido, mensagem, arquivo):
  db.query()