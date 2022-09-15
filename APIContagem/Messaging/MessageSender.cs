using System.Text.Json;
using Azure.Messaging.ServiceBus;

namespace APIContagem.Messaging;

public class MessageSender
{
    private readonly ILogger<MessageSender> _logger;
    private readonly IConfiguration _configuration;

    public MessageSender(ILogger<MessageSender> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    public async Task SendMessage<T>(T data)
    {
        var queueName = _configuration["AzureServiceBus:Queue"];
        var bodyContent = JsonSerializer.Serialize(data);

        var clientOptions = new ServiceBusClientOptions() { TransportType = ServiceBusTransportType.AmqpWebSockets };
        var client = new ServiceBusClient(
            _configuration.GetConnectionString("AzureServiceBus"), clientOptions);
        var sender = client.CreateSender(queueName);
        try
        {
            using var messageBatch = await sender.CreateMessageBatchAsync();
            var message = new ServiceBusMessage(bodyContent);
            if (!messageBatch.TryAddMessage(message))
                throw new Exception($"Mensagem grande demais para ser incluída no batch!");
                
            await sender.SendMessagesAsync(messageBatch);

            _logger.LogInformation(
                $"Azure Service Bus - Envio para a fila {queueName} concluído | " +
                $"{bodyContent}");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Falha na publicação da mensagem.");
            throw;
        }
        finally
        {
            if (client is not null)
            {
                await sender.CloseAsync();
                await sender.DisposeAsync();
                await client.DisposeAsync();

                _logger.LogInformation(
                    "Conexao com o Azure Service Bus fechada!");
            }
        }
    }
}