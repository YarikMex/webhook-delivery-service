package dev.webhookdelivery;

import java.net.URI;
import java.util.UUID;

import com.fasterxml.jackson.databind.node.ObjectNode;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
class DeliveryController {
    private final DeliveryQueue queue;

    DeliveryController(DeliveryQueue queue) {
        this.queue = queue;
    }

    @PostMapping("/events")
    ResponseEntity<AcceptedEvent> accept(@Valid @RequestBody IncomingEvent event) {
        UUID id = queue.enqueue(event.type(), event.data().toString());
        return ResponseEntity.accepted()
                .location(URI.create("/deliveries/" + id))
                .body(new AcceptedEvent(id));
    }

    @GetMapping("/deliveries/{id}")
    DeliveryQueue.Delivery find(@PathVariable UUID id) {
        return queue.find(id).orElseThrow(() ->
                new ResponseStatusException(HttpStatus.NOT_FOUND, "Delivery not found."));
    }

    record IncomingEvent(@NotBlank @Size(max = 120) @Pattern(regexp = "[A-Za-z0-9_.-]+") String type, @NotNull ObjectNode data) {}
    record AcceptedEvent(UUID id) {}
}
