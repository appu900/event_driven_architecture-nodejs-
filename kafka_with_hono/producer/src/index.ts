import { Hono } from "hono";
import { kafka, startServices } from "./services/start.services";

const app = new Hono();

startServices();

app.post("/produce", async (c) => {
  const { message } = await c.req.json();
  if (!message) {
    c.status(400);
    return c.json({ error: "Message is required" });
  }

  try {
    await kafka.produceMessage("test-topic", [{ value: message }]);
    c.status(201);
    return c.json({ message: "Message produced" });
  } catch (error: any) {
    c.status(500);
    return c.json({ error: error.message });
  }
});

export default app;

