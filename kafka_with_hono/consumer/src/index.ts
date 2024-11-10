import { Hono } from "hono";
import { startServices } from "./services/start.services";

const app = new Hono();

startServices();

app.get("/", (c) => {
  return c.text("Hello Hono!");
});

export default {
  port: 3001,
  fetch: app.fetch,
};
