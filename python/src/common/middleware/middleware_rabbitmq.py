from abc import abstractmethod
import pika
from .middleware import MessageMiddlewareCloseError, MessageMiddlewareDisconnectedError, MessageMiddlewareMessageError, MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

	def __init__(self, host, queue_name):
		self._connection = pika.BlockingConnection(pika.ConnectionParameters(host))
		self._channel = self._connection.channel()
		self._channel.queue_declare(queue=queue_name, durable=True, arguments={'x-queue-type': 'quorum'})

		self._queue_name = queue_name

		self._on_message_callback = None

		#Flags
		self._consuming = False
		self._consumer_tag = None

	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	# cada mensaje de datos o de control con el cuerpo del mensaje.
	# on_message_callback tiene como parámetros:
	# message - El valor tal y como lo recibe el método send de esta clase.
	# ack - Función que al invocarse realiza ack al mensaje que se está consumiendo.
	# nack - Función que al invocarse realiza nack al mensaje que se está consumiendo. 
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def start_consuming(self, on_message_callback):
		try:
			self._on_message_callback = on_message_callback
			self._consuming = True
			self._consumer_tag = self._channel.basic_consume(queue=self._queue_name,
                      on_message_callback=self._adapt_callback,
					  consumer_tag=self._consumer_tag)
			self._channel.start_consuming()
		except ConnectionError or pika.exceptions.AMQPConnectionError as e:
			raise MessageMiddlewareDisconnectedError("Connection Error") from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error") from e
		finally:
			self._on_message_callback = None
			self._consuming = False

	def _adapt_callback(self, ch, method, properties, body):
		def ack(): ch.basic_ack(delivery_tag=method.delivery_tag)
		def nack(): ch.basic_nack(delivery_tag=method.delivery_tag)
		self._on_message_callback(body, ack, nack)

	#Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	def stop_consuming(self):
		if self._consuming:
			try:
				self._channel.stop_consuming(consumer_tag=self._consumer_tag)
			except ConnectionError or pika.exceptions.AMQPConnectionError as e:
				raise MessageMiddlewareDisconnectedError("Connection Error") from e
			finally:
				self._consuming = False

	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	def send(self, message):
		try:
			self._channel.basic_publish(exchange='',routing_key=self._queue_name,body=message)
		except ConnectionError or pika.exceptions.AMQPConnectionError as e:
			raise MessageMiddlewareDisconnectedError("Connection Error") from e
		except Exception as e:
			raise MessageMiddlewareMessageError("Internal Error") from e

	#Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	def close(self):
		try:
			self._channel.close()
			self._connection.close()
		except Exception as e:
			raise MessageMiddlewareCloseError("Close Error") from e

		
class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

	def __init__(self, host, exchange_name, routing_keys):
		pass

	@abstractmethod
	def start_consuming(self, on_message_callback):
		pass

	@abstractmethod
	def stop_consuming(self):
		pass

	@abstractmethod
	def send(self, message):
		pass

	@abstractmethod
	def close(self):
		pass
