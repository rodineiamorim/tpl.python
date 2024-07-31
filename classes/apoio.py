import unicodedata
import re


def isset(objeto,propriedade):
  try:
    valor = objeto[propriedade]
    return 1
  except:
    return 0


def coalesce(objeto,propriedade,padrao):
  try:
    return objeto[propriedade]
  except:
    return padrao


def removeAcentos(palavra):

  # Unicode normalize transforma um caracter em seu equivalente em latin.
  nfkd = unicodedata.normalize('NFKD', palavra)
  palavraSemAcento = u"".join([c for c in nfkd if not unicodedata.combining(c)])

  # Usa expressão regular para retornar a palavra apenas com números, letras e espaço
  return re.sub('[^a-zA-Z0-9.,;/ \\\]', '', palavraSemAcento)


def removeMascara(texto):
  novo = texto
  novo = novo.replace("-","")
  novo = novo.replace(".","")
  novo = novo.replace(" ","")
  novo = novo.replace("(","")
  novo = novo.replace(")","")
  novo = novo.replace("\\","")
  novo = novo.replace("/","")
  return novo


def ocorrenciaPedido(db, idcliente, pedido, mensagem, arquivo):
  db.query()