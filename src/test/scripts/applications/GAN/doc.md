Simple GAN model:
This model was designed to be as simple as possible, mainly to efficiently test the training loop.

    Generator:

        Layer 1:
        
        Dense Layer with leaky relu activation: Takes the noisy input and increases the features.
                                                Leaky Relu is commonly used in GANs as it also allows for neagtive values.

        Layer 2:

        Dense Layer with tanh activation: Produces output in sahpe of the image.
                                          Tanh outputs values between -1 and 1.

    Discriminator:
        
        Layer 1:

        Dense layer with leaky relu activation: Takes images as input and decreases features.


        Layer 2:

        Dense layer with sigmoid activation: Transforms input to scalar value between 0 and 1.


CNN GAN model:
Commonly used GAN architecture; most models that can be found on the internet look similar to this.
Activation functions serve the same purposes as in the simple model.

    Generator:

        Layer 1:

        Dense Layer with batch normalization and leaky relu: Takes the noisy input and increases the features.
                                                             Batch normalization helps stabilizing the network.

        Layer 2:

        Conv 2D Transpose with batch normalization and leaky relu: Upsamples input.

        Layer 3:

        Conv 2D Transpose with batch normalization and leaky relu: Upsamples input.

        Layer 4:

        Conv 2D Transpose with batch normalization and tanh: Transforms input to image shape with values between -1 and 1.


    Discriminator:
    
        Layer 1:

        Conv 2D with leaky relu and dropout: Performs convolution on input images.
                                             Dropout is used for generalization.

        Layer 2:

        Conv 2D with leaky relu and dropout: Performs convolution on input.

        Layer 3:

        Dense layer with sigmoid activation: Transforms input to scalar value between 0 and 1.
